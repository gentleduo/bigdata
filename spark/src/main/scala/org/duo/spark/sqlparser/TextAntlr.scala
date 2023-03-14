package org.duo.spark.sqlparser


import org.antlr.v4.runtime.{ANTLRInputStream, CommonTokenStream}

/**
 * @Auther:duo
 * @Date: 2023-03-13 - 03 - 13 - 16:45
 * @Description: org.duo.spark.sqlparser
 * @Version: 1.0
 */
object TextAntlr {

  def main(args: Array[String]): Unit = {
    
    val lexer = new SqlParserLexer(new ANTLRInputStream("{1,2,3,4}"))
    val stream = new CommonTokenStream(lexer)
    val parser = new SqlParserParser(stream)
    val tree = parser.getRuleNames
    println(tree)
  }
}
