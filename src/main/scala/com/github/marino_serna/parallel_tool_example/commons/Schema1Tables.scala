package com.github.marino_serna.parallel_tool_example.commons

object Schema1Tables extends java.io.Serializable{

  case class Table1 (key: String,
                    field1: Int,
                    field2: String,
                    field3: String,
                    field4: String,
                    field5: String)

  case class Table2(key: String,
                    field1: Int,
                    field2: String,
                    field3: String,
                    field4: String,
                    field5: String)

  case class Table3(key: String,
                    field1: Int,
                    field2: String,
                    field3: String,
                    field4: String,
                    field5: String)
}
