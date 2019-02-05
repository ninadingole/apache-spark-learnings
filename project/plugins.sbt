import sbt._

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.1.0-M9")

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
classpathTypes += "maven-plugin"
