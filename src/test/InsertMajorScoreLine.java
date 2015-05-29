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

public class InsertMajorScoreLine {
	Statement stmt;
	String filePath;

	String[] pc = { "第一批A段", "第一批B段", "第二批A段", "第二批B段", "第三批", "专科批A段",
			"专科批B段", "民办专科批", "提前批", "贫困定向", "贫困定向", "特殊类型招生批" };

	public InsertMajorScoreLine(String file) {
		filePath = file;
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

	public int getSchoolID(String schoolName) {
		String sql;
		try {
			sql = "select schoolid from tschool where schoolname = '"
					+ schoolName + "' or usedName like '%" + schoolName + "%';";
			// System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next())
				return rs.getInt("schoolid");
			else
				return insertSchool(schoolName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	public int insertSchool(String schoolName) {
		String sql;
		try {
			sql = "insert into tschool(schoolname) values('" + schoolName
					+ "');";
			System.out.println(sql);
			stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
			ResultSet rs1 = stmt.getGeneratedKeys();
			if (rs1.next()) {
				return rs1.getInt(1);
			} else
				return 0;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	public int getMajorID(String majorName, int schoolID, int majorType,
			int majorEnrollType) {
		String sql;
		try {
			if (majorName.length() < 6)
				sql = "select count(*),majorid from tmajor where schoolid = "
						+ schoolID + " and  majorname like '" + majorName
						+ "%';";
			else
				sql = "select count(*),majorid from tmajor where schoolid = "
						+ schoolID + " and  majorname like '"
						+ majorName.substring(0, 6) + "%';";
			// System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			if (rs.getInt(1) == 0)
				return insertMajor(schoolID, majorName, majorType,
						majorEnrollType);
			return rs.getInt("majorid");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	public int insertMajor(int schoolID, String majorName, int majorType,
			int majorEnrollType) {
		String sql;
		try {
			sql = "insert into tmajor(schoolid,departmentid,majorname,majortype,majorenrolltype) values("
					+ schoolID
					+ ","
					+ schoolID
					+ ",'"
					+ majorName
					+ "',"
					+ majorType + "," + majorEnrollType + ");";
			System.out.println(sql);
			stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
			ResultSet rs1 = stmt.getGeneratedKeys();
			if (rs1.next()) {
				return rs1.getInt(1);
			} else
				return 0;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	public void insertIntoMajorScoreLine(int schoolID, int departmentID,
			int majorID, int year, double topScore, double bottomScore,
			double fTopScore, double fBottomScore, double pTopScore,
			double pBottomScore, double aTopScore, double aBottomScore,
			int enrollType) {
		// tmajorscoreline
		String sql;
		try {
			sql = "select count(1) from tmajorscoreline where year =" + year
					+ " and majorid = " + majorID + " ;";
			// System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			if (rs.getInt(1) == 0) {
				sql = "insert into tmajorscoreline values(0," + year + ","
						+ schoolID + "," + departmentID + "," + majorID
						+ ",0,0,0," + topScore + "," + bottomScore + ",0,"
						+ fTopScore + "," + fBottomScore + ",0," + pTopScore
						+ "," + pBottomScore + ",0," + aTopScore + ","
						+ aBottomScore + "," + enrollType + ");";
				System.out.println(sql);
				stmt.executeUpdate(sql);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	void processMajor() {
		long t1 = System.currentTimeMillis();

		File pDatFile = new File(filePath);
		if (pDatFile.exists() == true) {
			FileReader reader;
			try {
				int year, enrollType = 0, type = 0, schoolID, majorID;
				String schoolName, majorName, pc, lx;
				double t, b, ft, fb, pt, pb, at, ab;
				reader = new FileReader(filePath);
				BufferedReader bufferedReader = new BufferedReader(reader);
				String line = null;
				while ((line = bufferedReader.readLine()) != null) {
					year = Integer.parseInt(line.split("\t")[0]);
					schoolName = line.split("\t")[1];
					schoolID = getSchoolID(schoolName);
					pc = line.split("\t")[2];
					for (int i = 0; i < this.pc.length; i++)
						if (this.pc[i].equals(pc)) {
							enrollType = i + 1;
							break;
						}
					lx = line.split("\t")[3];
					if (!lx.startsWith("文史"))
						type = 1;
					majorName = line.split("\t")[4];
					majorID = getMajorID(majorName, schoolID, type, enrollType);
					t = Double.parseDouble(line.split("\t")[5]);
					b = Double.parseDouble(line.split("\t")[6]);
					ft = Double.parseDouble(line.split("\t")[7]);
					fb = Double.parseDouble(line.split("\t")[8]);
					pt = Double.parseDouble(line.split("\t")[9]);
					pb = Double.parseDouble(line.split("\t")[10]);
					at = Double.parseDouble(line.split("\t")[11]);
					ab = Double.parseDouble(line.split("\t")[12]);
					insertIntoMajorScoreLine(schoolID, schoolID, majorID, year,
							t, b, ft, fb, pt, pb, at, ab, enrollType);
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

	public static void main(String[] args) {
		InsertMajorScoreLine i = new InsertMajorScoreLine("major.txt");
		i.processMajor();
	}

}
