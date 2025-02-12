package org.apache.spark.deploy.history

import java.io.{BufferedReader, InputStreamReader}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

class LoginServlet extends HttpServlet{

  override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
    res.setContentType("text/html;charset=utf-8")
    // Serve the login.html page
    val htmlStream = getClass.getResourceAsStream("/org/apache/spark/ui/static/login.html")
    if (htmlStream != null) {
      val reader = new BufferedReader(new InputStreamReader(htmlStream))
      val writer = res.getWriter
      var line: String = null
      while ({ line = reader.readLine(); line != null }) {
        writer.println(line)
      }

      writer.close()
      reader.close()
    } else {
      res.sendError(HttpServletResponse.SC_NOT_FOUND, "Login page not found.")
    }
  }
}
