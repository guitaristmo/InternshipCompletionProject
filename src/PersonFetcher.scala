import io.getquill.QuillSparkContext._
import org.apache.spark.sql.SparkSession

class PersonFetcher {

  implicit lazy val sqlContext =
    SparkSession.builder().master("local").appName("spark test").getOrCreate().sqlContext
  import sqlContext.implicits._

  case class Employee(id: Int, firstName: String, lastName: String)
  case class EmployeeJobs(employeeID: Int, jobID: Int)
  case class Job(id: Int, title: String)
  case class JobDoer(jobTitle: String, employee: String)

  //employee table
  val employees = sqlContext.createDataset(Seq(
    Employee(0, "John", "Goldman"),
    Employee(1, "Sam", "GreenBerger"),
    Employee(2, "Simon", "Blumenthal")
  ))
  //jobs table
  val jobs = sqlContext.createDataset(Seq(
    Job(111, "Rebbi"),
    Job(222, "Administrator"),
    Job(613, "SeforimCleanup")
    ))
  val employeeJobs = sqlContext.createDataset(Seq(
    EmployeeJobs(0, 111),
    EmployeeJobs(0, 613),
    EmployeeJobs(1, 613),
    EmployeeJobs(2, 222)
  ))

  //query that produces table of JobDoer Objects
  val jobWorkers = run{
    liftQuery(employees)
      .join(liftQuery(employeeJobs))
      .on((emp, empJob) => emp.id == empJob.employeeID)
      .join(liftQuery(jobs))
      .on((eEj, j) => eEj._2.jobID == j.id)
      .map(eEjJ => JobDoer(eEjJ._2.title, eEjJ._1._1.firstName+" "+eEjJ._1._1.lastName))
  }
}

