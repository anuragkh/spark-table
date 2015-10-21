package amplab.spark.table

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterEach, Suite}

// TODO: delete this file and use the version from Spark once SPARK-750 is fixed.

/** Manages a local `sc` {@link SparkContext} variable, correctly stopping it after each test. */
trait LocalSparkContext extends BeforeAndAfterEach {
  self: Suite =>

  @transient var sc: SparkContext = _

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext() = {
    if (sc != null) {
      LocalSparkContext.stop(sc)
      sc = null
    }
  }

}

object LocalSparkContext {
  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T) = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

  def stop(sc: SparkContext) {
    sc.stop()
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

}
