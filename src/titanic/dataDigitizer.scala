package titanic

import scala.io.Source
import java.io.File
import java.io.PrintWriter
import java.sql.DriverManager
import java.sql.ResultSet

object dataDigitizer {
	
	def main(args: Array[String]) {
		
		classOf[com.mysql.jdbc.Driver]
		
		val localConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/kaggle?user=root&password=r00t")
		val localStatement = localConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
		
		var ticketMap: collection.mutable.Map[String, Integer] = collection.mutable.Map[String, Integer]()
		var cabinMap: collection.mutable.Map[String, Integer] = collection.mutable.Map[String, Integer]()
		
		case class TestingRow(override val line: String) extends TrainingRow("0," + line)
		
		case class TrainingRow(val line: String) {
			val pattern = """(\d+)\,(\d+)\,\"(.*)\"\,(.*)\,(.*)\,(\d+)\,(\d+)\,(.*)\,(.*)\,(.*)\,([CQS]{0,1}).*""".r
			//				  sur    pcla    name     sex   age   sibs   parc   tic   far   cab    embarked
			
			private val pattern(survival, pclass, name, sex, age_, sibsp, parch, ticket, fare_, cabin, embarked) = line
			
			def gender: Int = sex.toLowerCase() match {
				case "female" => 0
				case "male" => 1
				case _ => {
					println("gender - 1")
					-1
				}
			}
			
			def age: Float = age_ match {
				case "" => 0.0f
				case f => f.toFloat
			}
			
			def ticketNumber: Int = ticket match {
				case "" => -1
				case t => ticketMap.get(t) match {
					case Some(v) => v
					case None => {
						val value = if (cabinMap.values.isEmpty) 1 else cabinMap.values.max + 1
						ticketMap.put(t, value)
						value
					}
				}
			}
			
			def cabinNumber: Int = cabin match {
				case "" => 0
				case c => cabinMap.get(c) match {
					case Some(v) => v
					case None => {
						val value = if (cabinMap.values.isEmpty) 1 else cabinMap.values.max + 1
						cabinMap.put(c, value)
						value
					}
				}
			}
			
			def port: Int = embarked match {
				case "C" => 1
				case "Q" => 2
				case "S" => 3
				case _ => 0
			}
			
			def fare: Float = fare_ match {
				case "" => 0.0f
				case f => f.toFloat
			}
			
			val survived = survival.toInt
			val getPClass = pclass.toInt
			val getSibsp = sibsp.toInt
			val getParch = parch.toInt
			val getName = name
			val getTicket = ticket
			val getCabin = cabin
			val getEmbarked = embarked
			
			override def toString = {
				//"%d,%d,%f,%d,%d,%d,%f,%d,%d,%d".format(getPClass, gender, age, getSibsp, getParch, ticketNumber, fare, cabinNumber, port, survived + 1)
				
				/**
				 * data analysis shows that the survival rate depends on these fields:
				 * pclass
				 * sex
				 * sibsp
				 * parch
				 */
				
				"%d, %d, %d, %d, %d".format(getPClass, gender, getSibsp, getParch, survived + 1)
			} 
		}
		
		val trainingDataLocation = "/home/dotnot/data/datasci/kaggle/titanic/data/train.csv"
		val testingDataLocation = "/home/dotnot/data/datasci/kaggle/titanic/data/test.csv"
		val digitizedLocation  = "/home/dotnot/data/datasci/kaggle/titanic/data/digitized.csv"
		val testDigitizedLocation = "/home/dotnot/data/datasci/kaggle/titanic/data/test-digitized.csv"
			
		val stream = Source.fromFile(new File(trainingDataLocation)).getLines().filterNot(_.contains("embarked"))
		val output = new PrintWriter(new File(digitizedLocation), "UTF-8")
		
		val prepared = localConnection.prepareStatement("insert ignore into titanic_passengers(survival, pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		
		try {
		
			for (line <- stream) {
				
				val row = TrainingRow(line)
				output.println(row)
				
				prepared.setInt(1, row.survived)
				prepared.setInt(2, row.getPClass)
				prepared.setString(3, row.getName)
				prepared.setInt(4, row.gender)
				prepared.setFloat(5, row.age)
				prepared.setInt(6, row.getSibsp)
				prepared.setInt(7, row.getParch)
				prepared.setString(8, row.getTicket)
				prepared.setFloat(9, row.fare)
				prepared.setString(10, row.getCabin)
				prepared.setString(11, row.getEmbarked)
				
				prepared.addBatch()
			}
			
			prepared.executeBatch()
			
		} finally {
			output.close()
			localConnection.close()
		}
		
		val testStream = Source.fromFile(new File(testingDataLocation)).getLines().filterNot(_.contains("embarked"))
		val testOutput = new PrintWriter(new File(testDigitizedLocation), "UTF-8")
		
		try {
		
			for (line <- testStream) {
					
				val row = TestingRow(line)
				testOutput.println(row)
				
			}
			
		} finally {
			testOutput.close()
		}
		
	}
	
}