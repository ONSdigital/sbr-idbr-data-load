name := "sbr-enterprise_assembler"

version := "1.0"

scalaVersion := "2.11.8"

lazy val Versions = new {
  val hbase = "1.2.6"
  val spark = "2.1.0"
}

libraryDependencies ++= Seq(
  "org.apache.hbase"             %  "hbase-hadoop-compat"  % "1.2.1",
  "com.typesafe" % "config" % "1.3.2",
  ("org.apache.hbase"             %  "hbase-server"         % Versions.hbase)
    .exclude("com.sun.jersey","jersey-server"),
  "org.apache.hbase"             %  "hbase-common"         % Versions.hbase,
  "org.apache.hbase"             %  "hbase-client"         % Versions.hbase,
  ("org.apache.spark"             %% "spark-core"           % Versions.spark)
    .exclude("aopalliance","aopalliance")
    .exclude("commons-beanutils","commons-beanutils"),
  "org.apache.spark"             %% "spark-sql"            % Versions.spark

)


assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*)    => MergeStrategy.first
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.first
  case PathList("javax", "ws", xs @ _*) => MergeStrategy.first
  case PathList("jersey",".", xs @ _*) => MergeStrategy.first
  case PathList("aopalliance","aopalliance", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", xs @ _*)         => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

mainClass in (Compile,run) := Some("assembler.AssemblerMain")