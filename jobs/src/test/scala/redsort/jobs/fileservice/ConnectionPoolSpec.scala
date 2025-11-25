package redsort.jobs.fileservice

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.implicits.toTraverseOps
import io.grpc.Metadata
import redsort.AsyncSpec
import redsort.jobs.Common.NetAddr
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc

import scala.concurrent.duration._

class ConnectionPoolSpec extends AsyncSpec {
  def fixture = new {
    val registry: Map[Int, NetAddr] = Map(
      (1, NetAddr("1.1.1.1", 1111)), (2, NetAddr("2.2.2.2", 2222))
    )
    val config: ConnectionPoolConfig = ConnectionPoolConfig(2, 5.seconds)

    // stub factories
    val grpcClientFactory: NetAddr => Resource[IO, ReplicatorRemoteServiceFs2Grpc[IO, Metadata]] = {
      _ => Resource.eval(IO.pure(stub[ReplicatorRemoteServiceFs2Grpc[IO, Metadata]]))
    }
    val fileStorageFactory: NetAddr => Resource[IO, FileStorage] = {
      _ => Resource.eval(IO.pure(stub[FileStorage]))
    }

    // pool instance
    val pool = new ConnectionPoolAlgebraImpl(
      registry, config, grpcClientFactory, fileStorageFactory
    )
    val node1 = 1
    val node2 = 2

    val ensureEstablishment: FiniteDuration = 50.millis
  }

  behavior of "connectionPool.borrow"

  // NOTE: currently the number of active connection is not bounded (only the number of stored ones) TODO?
  it should "acquire a connection for a node" in {
    val f = fixture

    f.pool.borrow(f.node1).use { client =>
      IO {
        client should not be null
      }
    }
  }

  it should "not acquire a connection for a non-existent node" in {
    val f = fixture
    val fakeNode = 3
    println("Real nodes: " + f.node1 + ", " + f.node2)
    println("Try to borrow for " + fakeNode)

    assertThrows[NoSuchElementException] {
      f.pool.borrow(fakeNode).use(IO.pure).unsafeRunSync()
    }
  }

  it should "reuse a released connection" in {
    val f = fixture

    for {
      ref <- Ref.of[IO, Set[Int]](Set.empty)
      _ <- f.pool.borrow(f.node1).use { client =>
        ref.update(_ + client.hashCode())
      }
      _ <- f.pool.borrow(f.node1).use { client =>
        ref.get.map { used =>
          used.contains(client.hashCode()) shouldBe true
        }
      }
    } yield succeed
  }

  it should "handle multiple active connections up to the limit" in {
    val f = fixture
    val limit = f.config.connectionsPerMachine
    println("Limit is " + limit)

    for {
      release <- Deferred[IO, Unit]
      codes <- Ref.of[IO, List[Int]](Nil)

      // hold limit connections simultaneously
      fibers <- (1 to limit).toList.traverse { _ =>
        f.pool.borrow(f.node1).use { ctx =>
          for {
            h <- IO(ctx.hashCode())
            _ <- codes.update(h :: _)
            _ <- release.get // wait
          } yield ()
        }.start
      }

      // wait to ensure establishment of the connections
      _ <- IO.sleep(f.ensureEstablishment)

      active <- codes.get
      _ = println("Active: " + active.mkString(", "))
      unique = active.distinct
      _ = println("Unique: " + unique.mkString(", "))

      _ = unique.size shouldBe limit

      // release
      _ <- release.complete(())
      _ <- fibers.traverse(_.joinWithNever)
    } yield succeed
  }

  it should "not store connections beyond configured capacity" in {
    val f = fixture
    val limit = f.config.connectionsPerMachine
    println("Limit is " + limit)

    for {
      release <- Deferred[IO, Unit]
      codes <- Ref.of[IO, Set[Int]](Set.empty)

      // simultaneously borrow all + 1 more, storing their codes
      fibers <- (1 to (limit+1)).toList.traverse { _ =>
        f.pool.borrow(f.node1).use { ctx =>
          for {
            code <- IO(ctx.hashCode())
            _ <- codes.update(_ + code)
            _ <- release.get // wait
          } yield ()
        }.start
      }
      // wait to ensure establishment of the connections
      _ <- IO.sleep(f.ensureEstablishment)

      // sanity check -- created limit+1 unique
      codesSet <- codes.get
      _ = println("All established (limit+1): " + codesSet.mkString(", "))
      _ = codesSet.size shouldBe (limit+1)

      // release them all
      _ <- release.complete(())
      _ <- fibers.traverse(_.joinWithNever)

      // reset
      release <- Deferred[IO, Unit]
      reused <- Ref.of[IO, Set[Int]](Set.empty)

      // simultaneously borrow limit (should all be reused)
      fibers <- (1 to limit).toList.traverse { _ =>
        f.pool.borrow(f.node1).use { ctx =>
          for {
            code <- IO(ctx.hashCode())
            _ <- reused.update(_ + code)
            _ <- release.get // wait
          } yield ()
        }.start
      }
      // wait to ensure establishment of the connections
      _ <- IO.sleep(f.ensureEstablishment)

      // check the current connections
      reusedSet <- reused.get
      _ = println("Reused (limit): " + reusedSet.mkString(", "))
      _ = reusedSet.size shouldBe limit
      // see which one was not reused
      extra = codesSet.diff(reusedSet)
      _ = println("Not reused (1): " + extra.mkString(", "))
      _ = extra.size shouldBe 1

      // release them all
      _ <- release.complete(())
      _ <- fibers.traverse(_.joinWithNever)

      // borrow an additional one (should be from already reused)
      additional <- f.pool.borrow(f.node1).use { ctx =>
        IO.pure(ctx.hashCode())
      }
      _ = println("Additional: " + additional)

      _ = codesSet should contain (additional) // not an entirely new connection
      _ = reusedSet should contain (additional) // reused twice
      _ = additional should not equal extra // not (limit+1)th which should not have been stored
    } yield succeed
  }

  it should "not reuse connections across machines" in {
    val f = fixture

    for {
      // borrow and release from 2 nodes
      from1 <- f.pool.borrow(f.node1).use(IO.pure)
      from2 <- f.pool.borrow(f.node2).use(IO.pure)

      // borrow again
      from1Again <- f.pool.borrow(f.node1).use(IO.pure)
      from2Again <- f.pool.borrow(f.node2).use(IO.pure)
    } yield {
      from1.hashCode() shouldBe from1Again.hashCode()
      from2.hashCode() shouldBe from2Again.hashCode()
      from1.hashCode() should not equal from2.hashCode()
    }
  }

  behavior of "connectionPool.invalidateAllToMid"

  it should "invalidate all stored connections to a specific machine" in {
    val f = fixture
    val limit = f.config.connectionsPerMachine
    println("Limit is " + limit)

    for {
      release <- Deferred[IO, Unit]
      before <- Ref.of[IO, Set[Int]](Set.empty)

      // hold limit connections simultaneously
      fibers <- (1 to limit).toList.traverse { _ =>
        f.pool.borrow(f.node1).use { ctx =>
          for {
            code <- IO(ctx.hashCode())
            _ <- before.update(_ + code)
            _ <- release.get // wait
          } yield ()
        }.start
      }

      // wait to ensure establishment of the connections
      _ <- IO.sleep(f.ensureEstablishment)

      beforeSet <- before.get
      _ = println("Before: " + beforeSet.mkString(", "))

      // release
      _ <- release.complete(())
      _ <- fibers.traverse(_.joinWithNever)

      // invalidate
      _ <- f.pool.invalidateAllToMid(f.node1)

      // borrow again (expect new connections)
      release <- Deferred[IO, Unit]
      after <- Ref.of[IO, Set[Int]](Set.empty)

      // hold limit connections simultaneously
      fibers <- (1 to limit).toList.traverse { _ =>
        f.pool.borrow(f.node1).use { ctx =>
          for {
            code <- IO(ctx.hashCode())
            _ <- after.update(_ + code)
            _ <- release.get // wait
          } yield ()
        }.start
      }

      // wait to ensure establishment of the connections
      _ <- IO.sleep(f.ensureEstablishment)

      afterSet <- after.get
      _ = println("After: " + afterSet.mkString(", "))

      // release
      _ <- release.complete(())
      _ <- fibers.traverse(_.joinWithNever)

      reused = beforeSet.intersect(afterSet)
    } yield { reused.size shouldBe 0 }
  }

  it should "not invalidate connections to machines other than specified" in {
    val f = fixture

    for {
      // create and store some connections for different machines
      from1 <- f.pool.borrow(f.node1).use(IO.pure)
      _ <- f.pool.borrow(f.node2).use(IO.pure)

      _ <- f.pool.invalidateAllToMid(f.node2)

      // borrow from non-invalidated (no new one should be created)
      from1Again <- f.pool.borrow(f.node1).use(IO.pure)
    } yield { from1.hashCode() shouldBe from1Again.hashCode() }
  }

  it should "handle when invalidating for machine with no stored connections" in {
    val f = fixture

    for {
      // borrow some -- pool not empty afterwards
      _ <- f.pool.borrow(f.node1).use(IO.pure)

      // now invalidate -- should be empty afterwards
      _ <- f.pool.invalidateAllToMid(f.node1)

      // try to invalidate again (on an empty queue)
      _ <- f.pool.invalidateAllToMid(f.node1)
    } yield succeed
  }
}
