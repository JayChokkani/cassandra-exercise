package kduraj

import com.datastax.driver.core.Cluster

/**
 * Create Keyspace
 */
object Keyspace {

  def apply(IP: String, keyspace: String): Unit = {

    val clusterBuilder = Cluster.builder()
    clusterBuilder.addContactPoint(IP)
    clusterBuilder.withPort(9042)
    // clusterBuilder.withCredentials("admin", "password") // optional
    val cluster = clusterBuilder.build()
    val session = cluster.connect("system")

    val SQL =
      s"""
         | CREATE KEYSPACE  IF NOT EXISTS ${keyspace}
         | WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
        """.stripMargin

    println(SQL)
    session.execute(SQL)
    cluster.close()
    session.close()
  }

}
