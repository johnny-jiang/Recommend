package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class NewMajorAndSchool {
	Statement stmt;
	String filePath;
	int year;
	int majorType;
	int enrolltype;

	public int insertIntoSchool(String schoolName, int schoolType,
			String provinceID, String province, String city, String generalID,
			int enroll) {
		// tschool
		// tschoolgeneraltop
		// tdepartment
		String sql;
		int schoolID = -1;
		try {
			sql = "select  count(1),schoolid from tschool where schoolname = '"
					+ schoolName + "' or usedName like '%" + schoolName + "%';";
			// System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			if (rs.getInt(1) == 0) {
				sql = "insert into tschool(generalid,province,city,schoolname,schooltype,schoolenrooll) values(\""
						+ generalID
						+ "\",\""
						+ province
						+ "\",\""
						+ city
						+ "\",\""
						+ schoolName
						+ "\","
						+ schoolType
						+ ","
						+ enroll + ");";
				// System.out.println(sql);
				stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
				ResultSet rs1 = stmt.getGeneratedKeys();
				if (rs1.next()) {
					schoolID = rs1.getInt(1);
				}
				// System.out.println(schoolID);
				sql = "insert into tdepartment(schoolid,departmentname) values("
						+ schoolID + ",\"" + schoolName + "\");";
				// System.out.println(sql);
				stmt.executeUpdate(sql);
				sql = "insert into tschoolgeneraltop values(0," + schoolID
						+ ",\"" + generalID + "\",\"" + schoolName + "\",\""
						+ provinceID + "\");";
				// System.out.println(sql);
				stmt.executeUpdate(sql);
			} else {
				schoolID = rs.getInt(2);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return schoolID;
	}

	public int insertIntoMajor(int schoolID, int departmentID,
			String majorName, int majorType, String provinceID, int enrollType,
			int majorenroll, double schooling, String schoolName) {
		// tmajor
		// tmajorgeneraltop
		String sql;
		int majorID = -1;
		try {
			sql = "select count(1),majorid from tmajor where majorname ='"
					+ majorName + "' and schoolid = " + schoolID + ";";
			// System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			if (rs.getInt(1) == 0) {
				sql = "insert into tmajor(schoolid,departmentid,majorname,majortype,majorenrolltype,majorenroll,schooling,schoolname) values("
						+ schoolID
						+ ","
						+ departmentID
						+ ",'"
						+ majorName
						+ "',"
						+ majorType
						+ ","
						+ enrollType
						+ ","
						+ majorenroll
						+ ","
						+ schooling
						+ ",'"
						+ schoolName
						+ "');";
				// System.out.println(sql);
				stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
				ResultSet rs1 = stmt.getGeneratedKeys();
				if (rs1.next()) {
					majorID = rs1.getInt(1);
				}
				sql = "insert into tMajorGeneralToP values(0," + majorID
						+ ",0,'" + majorName + "',\"" + provinceID + "\");";
//				 System.out.println(sql);
				stmt.executeUpdate(sql);
			} else {
				majorID = rs.getInt(2);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return majorID;
	}

	public void insertIntoMajorScoreLine(int schoolID, int departmentID,
			int majorID, int year, double topScore, double bottomScore) {
		// tmajorscoreline
		String sql;
		try {
			sql = "select count(1) from tmajorscoreline where year =" + year
					+ " and majorid = " + majorID + " ;";
			// System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			if (rs.getInt(1) == 0) {
				sql = "insert into tmajorscoreline(year,schoolid,departmentid,majorid,topscore,bottomscore) values("
						+ year
						+ ","
						+ schoolID
						+ ","
						+ departmentID
						+ ","
						+ majorID + "," + topScore + "," + bottomScore + ");";
				// System.out.println(sql);
				stmt.executeUpdate(sql);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void process() {
		long t1 = System.currentTimeMillis();

		File pDatFile = new File(filePath);
		if (pDatFile.exists() == true) {
			FileReader reader;
			try {
				reader = new FileReader(filePath);
				BufferedReader bufferedReader = new BufferedReader(reader);
				String line = null;
				String schoolName, majorName, schoolProvinceID, majorProvinceID, schoolLocality, schoolGeneralID;
				int schoolEnroll, majorEnroll, schoolID;
				double schooling = 0;
				schoolID = 0;
				majorName = "";
				majorProvinceID = "";
				majorEnroll = 0;
				schooling = 0;
				schoolProvinceID = "";
				schoolName = "";
				schoolEnroll = 0;
				schoolLocality = "";
				schoolGeneralID = "";
				boolean majorInserted = true;
				while ((line = bufferedReader.readLine()) != null) {
					// System.out.println(line.trim());
					line = line.trim();
					int len = line.split("\\s+").length;
					// System.out.println(len);
					switch (len) {
					default:
					case 0:
						if (!majorInserted) {
							// System.out.println("insert.");
							insertIntoMajor(schoolID, schoolID, majorName,
									this.majorType, majorProvinceID,
									this.enrolltype, majorEnroll, schooling,
									schoolName);
							majorInserted = true;
						}
						break;
					case 1:
						majorName += line.trim();
						break;
					case 3:
						if (!majorInserted) {
							// System.out.println("insert.");
							insertIntoMajor(schoolID, schoolID, majorName,
									this.majorType, majorProvinceID,
									this.enrolltype, majorEnroll, schooling,
									schoolName);
							majorInserted = true;
						}
						// System.out.println(line.split("\\s+")[0].length());
						if (line.split("\\s+")[0].length() == 4) {
							schoolProvinceID = line.split("\\s+")[0];
							schoolName = line.split("\\s+")[1];
							schoolEnroll = Integer
									.parseInt(line.split("\\s+")[2]);
							String nextLine = bufferedReader.readLine().trim();
							schoolLocality = nextLine.substring(1,
									nextLine.length() - 1).split(",")[0];
							schoolGeneralID = nextLine.substring(1,
									nextLine.length() - 1).split(",")[1];
							schoolID = insertIntoSchool(schoolName, 0,
									schoolProvinceID, "", schoolLocality,
									schoolGeneralID, schoolEnroll);
							schoolProvinceID = "";
							schoolName = "";
							schoolEnroll = 0;
							schoolLocality = "";
							schoolGeneralID = "";
						} else {
							majorProvinceID = line.split("\\s+")[0];
							majorName = line.split("\\s+")[1];
							schooling = -1;
							majorEnroll = Integer
									.parseInt(line.split("\\s+")[2]);
							majorInserted = false;
						}
						break;
					case 4:
						if (!majorInserted) {
							// System.out.println("insert.");
							insertIntoMajor(schoolID, schoolID, majorName,
									this.majorType, majorProvinceID,
									this.enrolltype, majorEnroll, schooling,schoolName);
							majorInserted = true;
						}
						majorProvinceID = line.split("\\s+")[0];
						majorName = line.split("\\s+")[1];
						// System.out.println(majorName);
						if (line.split("\\s+")[2].equals("免收")
								|| line.split("\\s+")[2].equals("待定")
								|| line.split("\\s+")[2].startsWith("免费")
								|| line.split("\\s+")[2].equals("免费师"))
							schooling = -1;
						else
							schooling = Double
									.parseDouble(line.split("\\s+")[2]);
						majorEnroll = Integer.parseInt(line.split("\\s+")[3]);
						majorInserted = false;
						break;
					}
				}
			} catch (FileNotFoundException e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			} catch (IOException e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			}
		} else {
			System.out.println("file: " + filePath + " not exists!");
		}
		System.out.println("cost time :" + (System.currentTimeMillis() - t1)
				+ "ms!");

	}

	public void processT() {
		long t1 = System.currentTimeMillis();

		File pDatFile = new File(filePath);
		if (pDatFile.exists() == true) {
			FileReader reader;
			try {
				reader = new FileReader(filePath);
				BufferedReader bufferedReader = new BufferedReader(reader);
				String line = null;
				String schoolName, majorName, schoolProvinceID, majorProvinceID, schoolLocality, schoolGeneralID;
				int schoolEnroll, majorEnroll, schoolID;
				double schooling = 0;
				schoolID = 0;
				majorName = "";
				majorProvinceID = "";
				majorEnroll = 0;
				schooling = 0;
				schoolProvinceID = "";
				schoolName = "";
				schoolEnroll = 0;
				schoolLocality = "";
				schoolGeneralID = "";
				boolean majorInserted = true;
				while ((line = bufferedReader.readLine()) != null) {
					// System.out.println(line.trim());
					line = line.trim();
					int len = line.split("\\s+").length;
					// System.out.println(len);
					switch (len) {
					default:
					case 0:
						if (!majorInserted) {
							// System.out.println("insert.");
							insertIntoMajor(schoolID, schoolID, majorName,
									this.majorType, majorProvinceID,
									this.enrolltype, majorEnroll, schooling,schoolName);
							majorInserted = true;
						}
						break;
					case 1:
						majorName += line.trim();
						break;
					case 2:
						// System.out.println(line.split("\\s+")[0].length());
						if (line.split("\\s+")[0].length() == 4) {
							schoolProvinceID = line.split("\\s+")[0];
							schoolName = line.split("\\s+")[1];
							schoolEnroll = 0;
							String nextLine = bufferedReader.readLine().trim();
							schoolLocality = nextLine.substring(1,
									nextLine.length() - 1).split(",")[0];
							schoolGeneralID = nextLine.substring(1,
									nextLine.length() - 1).split(",")[1];
							schoolID = insertIntoSchool(schoolName, 0,
									schoolProvinceID, "", schoolLocality,
									schoolGeneralID, schoolEnroll);
							schoolProvinceID = "";
							schoolName = "";
							schoolEnroll = 0;
							schoolLocality = "";
							schoolGeneralID = "";
						} else {
							majorProvinceID = line.split("\\s+")[0];
							majorName = line.split("\\s+")[1];
							schooling = -1;
							majorEnroll = 0;
							majorInserted = false;
						}
						break;
					case 3:
						if (!majorInserted) {
							// System.out.println("insert.");
							insertIntoMajor(schoolID, schoolID, majorName,
									this.majorType, majorProvinceID,
									this.enrolltype, majorEnroll, schooling,schoolName);
							majorInserted = true;
						}
						majorProvinceID = line.split("\\s+")[0];
						majorName = line.split("\\s+")[1];
						// System.out.println(majorName);
						if (line.split("\\s+")[2].equals("待定")
								|| line.split("\\s+")[2].contains("免"))
							schooling = -1;
						else
							schooling = Double
									.parseDouble(line.split("\\s+")[2]);
						majorEnroll = 0;
						majorInserted = false;
						break;
					case 4:
						if (!majorInserted) {
							// System.out.println("insert.");
							insertIntoMajor(schoolID, schoolID, majorName,
									this.majorType, majorProvinceID,
									this.enrolltype, majorEnroll, schooling,schoolName);
							majorInserted = true;
						}
						majorProvinceID = line.split("\\s+")[0];
						majorName = line.split("\\s+")[1];
						// System.out.println(majorName);
						if (line.split("\\s+")[2].equals("免收")
								|| line.split("\\s+")[2].equals("待定")
								|| line.split("\\s+")[2].equals("免费")
								|| line.split("\\s+")[2].equals("免费师"))
							schooling = -1;
						else
							schooling = Double
									.parseDouble(line.split("\\s+")[2]);
						majorEnroll = 0;
						majorInserted = false;
						break;

					}

				}
			} catch (FileNotFoundException e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			} catch (IOException e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			}
		} else {
			System.out.println("file: " + filePath + " not exists!");
		}
		System.out.println("cost time :" + (System.currentTimeMillis() - t1)
				+ "ms!");

	}

	public NewMajorAndSchool() {
		try {
			Class.forName("com.mysql.jdbc.Driver"); // 加载MYSQL JDBC驱动程序
			// Class.forName("org.gjt.mm.mysql.Driver");
			System.out.println("Success loading Mysql Driver!");
		} catch (Exception e) {
			System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {
			Connection connect = DriverManager
					.getConnection(
							"jdbc:mysql://localhost:3306/recommend_test?useUnicode=true&characterEncoding=utf-8",
							"root", "");
			// 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
			System.out.println("Success connect Mysql server!");
			stmt = connect.createStatement();
		} catch (Exception e) {
			System.out.print("get data error!");
			e.printStackTrace();
		}

	}

	public NewMajorAndSchool(int year, int majortype, int enrolltype,
			String path) {
		this.year = year;
		this.filePath = path;
		this.majorType = majortype;
		this.enrolltype = enrolltype;
		try {
			Class.forName("com.mysql.jdbc.Driver"); // 加载MYSQL JDBC驱动程序
			// Class.forName("org.gjt.mm.mysql.Driver");
			System.out.println("Success loading Mysql Driver!");
		} catch (Exception e) {
			System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {
			Connection connect = DriverManager
					.getConnection(
							"jdbc:mysql://localhost:3306/recommend_test?useUnicode=true&characterEncoding=utf-8",
							"root", "");
			// 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
			System.out.println("Success connect Mysql server!");
			stmt = connect.createStatement();
		} catch (Exception e) {
			System.out.print("get data error!");
			e.printStackTrace();
		}
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// String sql =
		// "insert into tdepartment (schoolid,departmentname) values(1,\"abcde\");";
		// Statement stmt;
		// try {
		// Class.forName("com.mysql.jdbc.Driver"); // 加载MYSQL JDBC驱动程序
		// // Class.forName("org.gjt.mm.mysql.Driver");
		// System.out.println("Success loading Mysql Driver!");
		// } catch (Exception e) {
		// System.out.print("Error loading Mysql Driver!");
		// e.printStackTrace();
		// }
		// try {
		// Connection connect = DriverManager
		// .getConnection(
		// "jdbc:mysql://localhost:3306/db_recommand?useUnicode=true&characterEncoding=utf-8",
		// "root", "");
		// // 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
		// System.out.println("Success connect Mysql server!");
		// stmt = connect.createStatement();
		// stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
		// ResultSet rs = stmt.getGeneratedKeys();
		// if (rs.next()) {
		// int majorID = rs.getInt(1);
		// System.out.println(majorID);
		// }
		// } catch (Exception e) {
		// System.out.print("get data error!");
		// e.printStackTrace();
		// }

		String[] enrollTypeName = { "第一批A段", "第一批B段", "第二批A段", "第二批B段", "第三批",
				"专科批", "专科批B段", "民办专科批", "提前批", "贫困定向", };

		NewMajorAndSchool w1 = new NewMajorAndSchool(2014, 0, 1,
				"文史类 第一批A段.txt");
		System.out.println("start 文史类 第一批A段 data!");
		w1.process();
		System.out.println("文史类 第一批A段  data over!");

		NewMajorAndSchool w2 = new NewMajorAndSchool(2014, 0, 2,
				"文史类 第一批B段.txt");
		System.out.println("start 文史类 第一批B段 data!");
		w2.process();
		System.out.println("文史类 第一批B段  data over!");

		NewMajorAndSchool w3 = new NewMajorAndSchool(2014, 0, 3,
				"文史类 第二批A段.txt");
		System.out.println("start 文史类 第二批A段 data!");
		w3.process();
		System.out.println("文史类 第二批A段  data over!");

		NewMajorAndSchool w4 = new NewMajorAndSchool(2014, 0, 4,
				"文史类 第二批B段.txt");
		System.out.println("start 文史类第二批B段 data!");
		w4.process();
		System.out.println("文史类 第二批B段  data over!");

		NewMajorAndSchool w5 = new NewMajorAndSchool(2014, 0, 5, "文史类 第三批.txt");
		System.out.println("start 文史类 第三批 data!");
		w5.process();
		System.out.println("文史类 第三批  data over!");

		NewMajorAndSchool w6 = new NewMajorAndSchool(2014, 0, 6,
				"文史类 专科(高职)批.txt");
		System.out.println("start 文史类 专科(高职)批 data!");
		w6.process();
		System.out.println("文史类 专科(高职)批  data over!");

		NewMajorAndSchool w9 = new NewMajorAndSchool(2014, 0, 9, "文史类 提前批.txt");
		System.out.println("start 文史类 提前批 data!");
		w9.process();
		System.out.println("文史类 提前批  data over!");

		NewMajorAndSchool w10 = new NewMajorAndSchool(2014, 0, 10,
				"国家面向贫困地区定向招生专项计划和地方(省属)重点高校农村专项计划.txt");
		System.out.println("start 文史类 贫困定向 data!");
		w10.process();
		System.out.println("文史类 贫困定向  data over!");

		NewMajorAndSchool w12 = new NewMajorAndSchool(2014, 0, 12,
				"文史类 特殊类型招生批.txt");
		System.out.println("start 文史类 特殊类型招生批 data!");
		w12.processT();
		System.out.println("文史类特殊类型招生批  data over!");

		NewMajorAndSchool l1 = new NewMajorAndSchool(2014, 1, 1,
				"理工农医类-第一批A段.txt");
		System.out.println("start 理工农医类 第一批A段 data!");
		l1.process();
		System.out.println("理工农医类 第一批A段  data over!");

		NewMajorAndSchool l2 = new NewMajorAndSchool(2014, 1, 2,
				"理工农医类-第一批B段.txt");
		System.out.println("start 理工农医类 第一批B段 data!");
		l2.process();
		System.out.println("理工农医类 第一批B段  data over!");

		NewMajorAndSchool l3 = new NewMajorAndSchool(2014, 1, 3,
				"理工农医类-第二批A段.txt");
		System.out.println("start 理工农医类 第二批A段 data!");
		l3.process();
		System.out.println("理工农医类 第二批A段  data over!");

		NewMajorAndSchool l4 = new NewMajorAndSchool(2014, 1, 4,
				"理工农医类-第二批B段.txt");
		System.out.println("start 理工农医类第二批B段 data!");
		l4.process();
		System.out.println("理工农医类 第二批B段  data over!");

		NewMajorAndSchool l5 = new NewMajorAndSchool(2014, 1, 5,
				"理工农医类-第三批.txt");
		System.out.println("start 理工农医类 第三批 data!");
		l5.process();
		System.out.println("理工农医类 第三批  data over!");

		NewMajorAndSchool l6 = new NewMajorAndSchool(2014, 1, 6,
				"理工农医类-专科(高职)批.txt");
		System.out.println("start 理工农医类 专科(高职)批 data!");
		l6.process();
		System.out.println("理工农医类 专科(高职)批  data over!");

		NewMajorAndSchool l9 = new NewMajorAndSchool(2014, 1, 9,
				"理工农医类-提前批.txt");
		System.out.println("start 理工农医类 提前批 data!");
		l9.process();
		System.out.println("理工农医类 提前批  data over!");

		NewMajorAndSchool l10 = new NewMajorAndSchool(2014, 1, 10,
				"理工农医类-国家面向贫困地区定向招生专项计划和地方(省属)重点高校农村专项计划.txt");
		System.out.println("start 理工农医类 贫困定向 data!");
		l10.process();
		System.out.println("理工农医类 贫困定向  data over!");

		NewMajorAndSchool l12 = new NewMajorAndSchool(2014, 1, 12,
				"理工农医类-特殊类型招生批.txt");
		System.out.println("start 理工农医类 特殊类型招生批 data!");
		l12.processT();
		System.out.println("理工农医类特殊类型招生批  data over!");

	}

}
