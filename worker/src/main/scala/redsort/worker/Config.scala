package redsort.worker.config

case class Config(
    address: String = "",
    inputs: Seq[String] = Seq(),
    output: String = ""
)
