package at.bronzels.sparkmodels

import java.util.regex.Pattern

object HtmlUtil {
  private val regEx_script = "<script[^>]*?>[\\s\\S]*?<\\/script>" // 定义script的正则表达式
  private val regEx_style = "<style[^>]*?>[\\s\\S]*?<\\/style>" // 定义style的正则表达式
  private val regEx_html = "<[^>]+>" // 定义HTML标签的正则表达式
  private val regEx_space = "\\s*|\t|\r|\n" // 定义空格回车换行符

  /**
   * @param htmlStr
   * @return 删除Html标签
   */
  def delHTMLTag(htmlStr: String): String = {
    var ret = htmlStr
    
    val p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE)
    val m_script = p_script.matcher(ret)
    ret = m_script.replaceAll("") // 过滤script标签

    val p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE)
    val m_style = p_style.matcher(ret)
    ret = m_style.replaceAll("") // 过滤style标签

    val p_html = Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE)
    val m_html = p_html.matcher(ret)
    ret = m_html.replaceAll("") // 过滤html标签

    val p_space = Pattern.compile(regEx_space, Pattern.CASE_INSENSITIVE)
    val m_space = p_space.matcher(ret)
    ret = m_space.replaceAll("") // 过滤空格回车标签

    ret.trim // 返回文本字符串

  }

  def getTextFromHtml(htmlStr: String): String = {
    var ret = htmlStr

    ret = delHTMLTag(ret)
    ret = ret.replaceAll(" ", "")
    ret
  }
}
