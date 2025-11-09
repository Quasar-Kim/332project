## Directories

The project is composed of multiple subprojects.

- `redsort`: Top level project, no actual code here.
    - `redsort.jobs`: Provides automata and data structures for job scheduling system. (in `jobs` directory)
    - `redsort.master`: Master program implementation. (in `master` directory)
    - `redosrt.worker`: Worker program implementation. (in `worker` directory)

`redsort.jobs` is a purely functional and IO-free implementation of what we designed. We can implement stateful automata as a "step" function that accepts a "state" object as argument and returns new state object along with some data. It should not provide any  kind of network or disk IO, but rather return some kind of "message" that instructs caller to do actual IO.

`redsort.master` and `redsort.worker` uses functions and data structures provide by `redsort.jobs` package to implement actual scheduler and worker.

Protocol buffers definitions resides in `redsort.jobs.messages` package, and source files are located in `jobs/src/main/protobuf`.

## SBT Tips

- Use `projects` to list all available projects, then use `project <name>` to change the project.
- You can call commands of subprojects by using slash (/). For example, in top level project you can compile `redsort.jobs` package with `jobs/compile` command.
- Read [the sbt manual on suprojects](https://www.scala-sbt.org/1.x/docs/Multi-Project.html).

## Deploying

Run `assembly` job to create jar file for master and worker. For example, `master.jar` will be generated at `jobs/target/scala_2.13` directory. In production we can write a small script named `master` that runs `java -jar master.jar` (with command line arguments).