package utils

object IPUtils {
  def ip2Long(ip:String): Long = {
    var strings: Array[String] = ip.split("[.]")
    var ipNum = 0l

    for(i<-0 until(strings.length)){
      ipNum = strings(i).toLong | ipNum << 8L
    }

    return ipNum
  }

  def main(args: Array[String]): Unit = {
    print(ip2Long("182.91.190.221"))
  }
}
