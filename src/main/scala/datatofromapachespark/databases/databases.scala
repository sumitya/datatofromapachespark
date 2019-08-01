package datatofromapachespark

import datatofromapachespark.utils.GetAllProperties

package object databases {

  val userName = System.getProperty("user.name")

  val allProperties = GetAllProperties.readPropertyFile

  private[databases] val DBTABLE = allProperties getOrElse("DBTABLE","#")
  private[databases] val URL = allProperties getOrElse("URL","#")
  private[databases] val USER = allProperties getOrElse("USER","#")
  private[databases] val PASSWORD = allProperties getOrElse("PASSWORD","#")
  private[databases] val NUMPARTITIONS = allProperties getOrElse("NUMPARTITIONS","#")
  private[databases] val PARTITIONCOLUMN = allProperties getOrElse("PARTITIONCOLUMN","#")
  private[databases] val LOWERBOUND = allProperties getOrElse("LOWERBOUND","#")
  private[databases] val UPPERBOUND = allProperties getOrElse("UPPERBOUND","#")
  private[databases] val FETCHSIZE = allProperties getOrElse("FETCHSIZE","#")

}
