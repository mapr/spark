package org.apache.spark.deploy.history

import javax.servlet.http.{Cookie, HttpServlet, HttpServletRequest, HttpServletResponse}

class LogoutServlet extends HttpServlet {
  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val session = req.getSession(false)
    if (session != null) {
      session.invalidate()
    }

    val cookies = req.getCookies
    if (cookies != null) {
      cookies.foreach(cookie => {
        cookie.setValue("")
        cookie.setPath(req.getContextPath + "/")
        cookie.setMaxAge(0)
        resp.addCookie(cookie)
      })
    }

    val logoutCookie = new Cookie("logout", "true")
    logoutCookie.setPath("/")
    logoutCookie.setHttpOnly(true)
    logoutCookie.setSecure(true)
    resp.addCookie(logoutCookie)

    resp.sendRedirect("/")
  }

}
