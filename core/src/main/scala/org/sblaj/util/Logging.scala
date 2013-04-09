package org.sblaj.util

import java.util.Date

trait Logging {
  def info(msg: => String) {
    println(new Date() + " " + msg)
  }
}
