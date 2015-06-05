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

public class InsertSchoolScoreLine {
	Statement stmt;
	String filePath;

	String[] pc = { "第一批A段", "第一批B段", "第二批A段", "第二批B段", "第三批", "专科批A段",
			"专科批B段", "民办专科批", "提前批", "贫困定向", "贫困定向", "特殊类型招生批" };

	public InsertSchoolScoreLine(String file) {
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
			// System.out.println(sql);
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

	public void insertIntoSchoolScoreLine(int schoolID, int year, int type,
			int enrollType, int count, double topScore, double bottomScore,
			int fcount, double fTopScore, double fBottomScore, int pcount,
			double pTopScore, double pBottomScore, int acount,
			double aTopScore, double aBottomScore) {
		String sql;
		try {
			sql = "insert into tschoolscoreline values(0," + schoolID + ","
					+ year + "," + type + "," + enrollType + "," + count + ","
					+ topScore + "," + bottomScore + "," + fcount + ","
					+ fTopScore + "," + fBottomScore + "," + pcount + ","
					+ pTopScore + "," + pBottomScore + "," + acount + ","
					+ aTopScore + "," + aBottomScore + ");";
			// System.out.println(sql);
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	void processSchool() {
		long t1 = System.currentTimeMillis();

		File pDatFile = new File(filePath);
		if (pDatFile.exists() == true) {
			FileReader reader;
			try {
				int schoolID, year, type = 0, enrollType = 0, count, fcount, pcount, acount;
				double topScore, bottomScore, ftopScore, fbottomScore, ptopScore, pbottomScore, atopScore, abottomScore;
				String schoolName, pc, lx;
				reader = new FileReader(filePath);
				BufferedReader bufferedReader = new BufferedReader(reader);
				String line = null;
				while ((line = bufferedReader.readLine()) != null) {
					// System.out.println(line);
					year = Integer.parseInt(line.split("\t")[0]);
					schoolName = line.split("\t")[1];
					pc = line.split("\t")[2];
					for (int i = 0; i < this.pc.length; i++)
						if (this.pc[i].equals(pc)) {
							enrollType = i + 1;
							break;
						}
					if (enrollType == 7 || enrollType == 8)
						enrollType = 6;
					lx = line.split("\t")[3];
					if (!lx.startsWith("文史"))
						type = 1;
					count = Integer.parseInt(line.split("\t")[4]);
					topScore = Double.parseDouble(line.split("\t")[5]);
					bottomScore = Double.parseDouble(line.split("\t")[6]);
					fcount = Integer.parseInt(line.split("\t")[7]);
					ftopScore = Double.parseDouble(line.split("\t")[8]);
					fbottomScore = Double.parseDouble(line.split("\t")[9]);
					pcount = Integer.parseInt(line.split("\t")[10]);
					ptopScore = Double.parseDouble(line.split("\t")[11]);
					pbottomScore = Double.parseDouble(line.split("\t")[12]);
					acount = Integer.parseInt(line.split("\t")[13]);
					atopScore = Double.parseDouble(line.split("\t")[14]);
					abottomScore = Double.parseDouble(line.split("\t")[15]);
					schoolID = getSchoolID(schoolName);
					insertIntoSchoolScoreLine(schoolID, year, type, enrollType,
							count, topScore, bottomScore, fcount, ftopScore,
							fbottomScore, pcount, ptopScore, pbottomScore,
							pcount, atopScore, abottomScore);
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
		InsertSchoolScoreLine i = new InsertSchoolScoreLine(
				"2011school.txt");
		i.processSchool();
		i = new InsertSchoolScoreLine("2012school.txt");
		i.processSchool();
		i = new InsertSchoolScoreLine("2013school.txt");
		i.processSchool();
		i = new InsertSchoolScoreLine("2014school.txt");
		i.processSchool();
	}

}
