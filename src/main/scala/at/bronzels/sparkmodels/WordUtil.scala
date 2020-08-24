package at.bronzels.sparkmodels

import cn.hutool.core.util.NumberUtil

object WordUtil {
  def isNumber(input: String): Boolean = {
    NumberUtil.isNumber(input)
  }

  def isWithChinese(input: String): Boolean = {
    for(ch <- input) {
      if (ch >= '\u4e00' && ch <= '\u9fa5')
        return true
    }
    false
  }

  def isLetterDigit(input: String): Boolean = {
    val regex = "^[a-z0-9A-Z]+$"
    return input.matches(regex)
  }

  def main(args: Array[String]): Unit = {
    System.out.println("test is_contains_chinese")
    System.out.println(isWithChinese("你"))
    System.out.println(isWithChinese("fo我o"))
    System.out.println(isWithChinese("12我3"))
    System.out.println(isWithChinese("1.3我"))
    System.out.println(isWithChinese("foo"))
    System.out.println(isWithChinese("123"))
    System.out.println(isWithChinese("1.3"))

    System.out.println("test is_number")
    System.out.println(isNumber("你我"))
    System.out.println(isNumber("foo"))
    System.out.println(isNumber("123"))
    System.out.println(isNumber("1.3"))
    System.out.println(isNumber("-1.37"))
    System.out.println(isNumber("1e3"))
    System.out.println(isNumber("-1.2e4"))
  }
}
