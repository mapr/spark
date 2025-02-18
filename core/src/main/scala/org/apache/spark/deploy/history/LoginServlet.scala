package org.apache.spark.deploy.history

import java.io.{BufferedReader, InputStreamReader}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.util.Using

class LoginServlet extends HttpServlet {

  private val loginPagePath = "/org/apache/spark/ui/static/login.html"

  override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
    res.setContentType("text/html;charset=utf-8")

    val htmlStream = getClass.getResourceAsStream(loginPagePath)

    if (htmlStream != null) {
      Using.resource(new BufferedReader(new InputStreamReader(htmlStream))) { reader =>
        val writer = res.getWriter
        try {
          var line: String = null
          while ({ line = reader.readLine(); line != null }) {
            writer.println(line)
          }
        } finally {
          writer.close()
        }
      }
    } else {
      res.sendError(HttpServletResponse.SC_NOT_FOUND, "Login page not found.")
    }
  }
}
