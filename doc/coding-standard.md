Junseong suggested:

## Use algebraic data types

Algebraic data types are composite data types made with "AND" and "OR" operation. Type made with "AND" operation is called "product type", and one made with "OR" is called "sum type".

Product type can be written using `final case class` in scala:

```scala
final case class ProductType(x: Int, y: String)
```

And sum types are represented by a `sealed abstract class`. This is similar to enums in different languages.

```scala
sealed abstract class SumType
final case class A() extends SumType
final case class B() extends SumType
```

I suggest to use these data types because they works well with purely functinal code.

## No side effects

Try to minimize side effects, especially in `jobs` package.

## Formatting

The project is configured with `scalafmt`. Sbt will automatically format all source files in compilation. You can also manually format your project with IDE formatter integration or with `scalafmtAll` command in sbt console.