package test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySQL_JDBC_TEST {

	class majorscoreline {
		int id;
		int schooIDd;
		int departmentiD;
		int majoriD;
		double schooling;
		int enrollPlan;
		int actuallyEnrool;
		double topScore;
		double bottomScore;
		int firstCount;
		double firstTopScore;
		double firstBottomScore;
		int parallenlCount;
		double parallelTopScore;
		double parallelBottomScore;
		int additionalCount;
		double additionalTopScore;
		double additionalBottomScore;
		int year;
		int generalCode;
		int majorName;
		int majorCode;
		int majorRate;
		int iskeyMajor;
		int majorRank;
		int majorType;
		int majorClass;
		int majorEnrolType;
		int majorEnroll;
		String majorInfo;
	}

	class school {
		int schoolid;
		int localityP;
		int localityC;
		String schoolName;
		String address;
		String website;
		int schoolType;
		String schoolInfo;
		boolean isBase;
		int schoolHasKeyMajor;
		boolean isMinistry;
		boolean is211;
		boolean is985;
		boolean hasGraduate;
		boolean isProvincial;
		String schoolCode;
		int schoolRank;
		double schoolRRate;
		String keyMajorDetail;
		int schoolRnroll;
		String filed1;
		String filed2;
	}

	void recommand(int score, int risk, int year) {
		try {
			Class.forName("com.mysql.jdbc.Driver"); // 加载MYSQL JDBC驱动程序
			// Class.forName("org.gjt.mm.mysql.Driver");
			System.out.println("Success loading Mysql Driver!");
		} catch (Exception e) {
			System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {
			String sql;
			int localityp = 0;
			int localityc = 0;
			int majortype = 0;
			int majorclass = 0;
			int majorenrolltype = 0;
			int is211 = 0;
			int is985 = 0;
			int generalcode = 0;
			double safeDown = -3, safeUp = 5;
			double normalDown = -5, normalUp = 10;
			double riskDown = -10, riskUp = 15;
			ResultSet rs;
			Statement stmt;
			int ranktop, rankbottom;
			double top, bottom;
			Connection connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/db_recommand", "root", "");
			// 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
			System.out.println("Success connect Mysql server!");
			stmt = connect.createStatement();
			long t1 = System.currentTimeMillis();

			sql = "select count from vrecorded where year=" + year
					+ " and risk = " + risk + " and score = " + score
					+ " and type = " + majortype + ";";
			System.out.println(sql);
			rs = stmt.executeQuery(sql);
			rs.next();
			if (!rs.first()) {
				System.out.println("dataset is null , insert data.");
				sql = "select ranktop,rankbottom from tscoretorank where score="
						+ score
						+ " and year="
						+ year
						+ " and type="
						+ majortype + ";";
				System.out.println(sql);
				rs = stmt.executeQuery(sql);
				rs.next();
				ranktop = rs.getInt("ranktop");
				rankbottom = rs.getInt("rankbottom");
				System.out.println("ranktop:" + ranktop + "\nrankbottom:"
						+ rankbottom);
				sql = "select score from tscoretorank where year = " + year
						+ " and type = " + majortype + " and ranktop <="
						+ ranktop + " and rankbottom > " + ranktop + ";";
				rs = stmt.executeQuery(sql);
				rs.next();
				top = rs.getDouble("score");
				System.out.println("top score:" + top);
				sql = "select score from tscoretorank where year = " + year
						+ " and type = " + majortype + " and  ranktop <"
						+ rankbottom + " and rankbottom >= " + rankbottom + ";";
				rs = stmt.executeQuery(sql);
				rs.next();
				bottom = rs.getDouble("score");
				System.out.println("bottom score:" + bottom);

				switch (risk) {
				case 0:
					sql = "insert into trecord(year,risk,score,type,schoolid,departmentid,resultid) select "
							+ year
							+ ","
							+ risk
							+ ","
							+ score
							+ ","
							+ majortype
							+ ",schoolid,departmentid,id from tmajorscoreline where year = "
							+ year
							+ " and topscore between "
							+ (double) (top + safeDown)
							+ " and "
							+ (double) (top + safeUp)
							+ " and topscore+bottomscore between "
							+ (double) 2 * (bottom + safeDown)
							+ " and "
							+ (double) 2 * (bottom + safeUp)
							+ " and majorid in (select majorid from tmajor where majortype = "
							+ majortype + ");";
					break;
				default:
				case 1:
					sql = "insert into trecord(year,risk,score,type,schoolid,departmentid,resultid) select "
							+ year
							+ ","
							+ risk
							+ ","
							+ score
							+ ","
							+ majortype
							+ ",schoolid,departmentid,id from tmajorscoreline where year = "
							+ year
							+ " and bottomscore*3/4+ topscore/4 between "
							+ (double) (bottom + normalDown)
							+ " and "
							+ (double) (bottom + normalUp)
							+ " and topscore*3/4 + bottomscore/4 between "
							+ (double) (top + normalDown)
							+ " and "
							+ (double) (top + normalUp)
							+ " and majorid in (select majorid from tmajor where majortype = "
							+ majortype + ");";
					break;
				case 2:
					sql = "insert into trecord(year,risk,score,type,schoolid,departmentid,resultid) select "
							+ year
							+ ","
							+ risk
							+ ","
							+ score
							+ ","
							+ majortype
							+ ",schoolid,departmentid,id from tmajorscoreline where year = "
							+ year
							+ " and bottomscore between "
							+ (double) (bottom + riskDown)
							+ " and "
							+ (double) (bottom + riskUp)
							+ " and topscore + bottomscore between "
							+ (double) 2 * (top + riskDown)
							+ " and "
							+ (double) 2 * (top + riskUp)
							+ " and majorid in (select majorid from tmajor where majortype = "
							+ majortype + ");";
					break;
				}
				System.out.println(sql);
				stmt.executeUpdate(sql);
				if (stmt.getUpdateCount() > 0)
					System.out.println("insert scuess!\nupdate count : "
							+ stmt.getUpdateCount());
			} else {
				System.out.println("data set exists.");
				int count = rs.getInt("count");
				System.out.println("data count: " + count);
			}
			sql = "select * from tmajorscoreline where majorid in (select resultid from trecord where year = "
					+ year
					+ " and risk = "
					+ risk
					+ " and score = "
					+ score
					+ " and type = "
					+ majortype
					+ ") and schoolid in (select schoolid from tschool where localityp = "
					+ localityp
					+ " and is211 = "
					+ is211
					+ " and is985 = "
					+ is985
					+ ") and majorid in(select majorid from tmajor where majortype = "
					+ majortype
					+ " and majorclass = "
					+ majorclass
					+ " and majorenrolltype = "
					+ majorenrolltype
					+ " and generalcode =" + generalcode + " );";
			System.out.println(sql);

			ResultSet scoreLineRS = stmt.executeQuery(sql);
			List<Integer> majoridList = new ArrayList<Integer>();
			List<Integer> schoolidList = new ArrayList<Integer>();
			List<Integer> departmentidList = new ArrayList<Integer>();
			int majorid, schoolid, departmentid;
			System.out.println("major score line :");
			while (scoreLineRS.next()) {
				majorid = scoreLineRS.getInt("majorid");
				departmentid = scoreLineRS.getInt("departmentid");
				schoolid = scoreLineRS.getInt("schoolid");
				if (!majoridList.contains(majorid))
					majoridList.add(majorid);
				if (!departmentidList.contains(departmentid))
					departmentidList.add(departmentid);
				if (!schoolidList.contains(schoolid))
					schoolidList.add(schoolid);
				System.out.println("school id: "
						+ scoreLineRS.getInt("schoolid") + " department id: "
						+ scoreLineRS.getInt("departmentid") + " id: "
						+ scoreLineRS.getInt("id") + " schooling: "
						+ scoreLineRS.getDouble("schooling"));
			}

			System.out.println("school data :");
			sql = "select * from tschool where schoolid in (";
			for (int id : schoolidList) {
				sql += id + ",";
			}
			sql = sql.substring(0, sql.length() - 1);
			sql += ");";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("school id: " + rs.getInt("schoolid")
						+ " name: " + rs.getString("schoolname"));
			}
			rs.last();
			System.out.println("school count: "+rs.getRow());
			
			System.out.println("department data :");
			sql = "select * from tdepartment where departmentid in (";
			for (int id : departmentidList) {
				sql += id + ",";
			}
			sql = sql.substring(0, sql.length() - 1);
			sql += ");";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("school id: " + rs.getInt("schoolid")
						+ " department id: " + rs.getInt("departmentid")
						+ " name: " + rs.getString("departmentname"));
			}
			rs.last();
			System.out.println("department count: "+rs.getRow());

			System.out.println("major data :");
			sql = "select * from tmajor where majorid in (";
			for (int id : majoridList) {
				sql += id + ",";
			}
			sql = sql.substring(0, sql.length() - 1);
			sql += ");";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("school id: " + rs.getInt("schoolid")
						+ " department id: " + rs.getInt("departmentid")
						+ " major id: " + rs.getInt("majorid") + " name: "
						+ rs.getString("majorname"));
			}
			rs.last();
			System.out.println("major count: "+rs.getRow());

			System.out.println("cost time :"
					+ (System.currentTimeMillis() - t1) + "ms!");

		} catch (Exception e) {
			System.out.print("get data error!");
			e.printStackTrace();
		}
	}

	void search() {
		try {
			Class.forName("com.mysql.jdbc.Driver"); // 加载MYSQL JDBC驱动程序
			// Class.forName("org.gjt.mm.mysql.Driver");
			System.out.println("Success loading Mysql Driver!");
		} catch (Exception e) {
			System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {
			String sql;
			int year = 2013;
			int schoolID = 3;
			int majortype = 1;
			int majorclass = 0;
			int majorenrolltype = 0;
			int generalcode = 0;
			ResultSet rs;
			Statement stmt;
			Connection connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/db_recommand", "root", "");
			// 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
			System.out.println("Success connect Mysql server!");
			stmt = connect.createStatement();
			long t1 = System.currentTimeMillis();

			sql = "select * from tmajorscoreline where year = "
					+ year
					+ " and schoolid = "
					+ schoolID
					+ " and majorid in(select majorid from tmajor where majortype = "
					+ majortype + " and majorclass = " + majorclass
					+ " and majorenrolltype = " + majorenrolltype
					+ " and generalcode = " + generalcode + " );";
			rs = stmt.executeQuery(sql);

			List<Integer> majoridList = new ArrayList<Integer>();
			List<Integer> schoolidList = new ArrayList<Integer>();
			List<Integer> departmentidList = new ArrayList<Integer>();
			int majorid, schoolid, departmentid;
			while (rs.next()) {
				majorid = rs.getInt("majorid");
				departmentid = rs.getInt("departmentid");
				schoolid = rs.getInt("schoolid");
				if (!majoridList.contains(majorid))
					majoridList.add(majorid);
				if (!departmentidList.contains(departmentid))
					departmentidList.add(departmentid);
				if (!schoolidList.contains(schoolid))
					schoolidList.add(schoolid);
				// System.out.println("school id: " + rs.getInt("schoolid")
				// + " department id: " + rs.getInt("departmentid")
				// + " id: " + rs.getInt("id") + " schooling: "
				// + rs.getDouble("schooling"));
			}

			System.out.println("school data :");
			sql = "select * from tschool where schoolid in (";
			for (int id : schoolidList) {
				sql += id + ",";
			}
			sql = sql.substring(0, sql.length() - 1);
			sql += ");";
			System.out.println(sql);
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("school id: " + rs.getInt("schoolid")
						+ " name: " + rs.getString("schoolname"));
			}

			System.out.println("department data :");
			sql = "select * from tdepartment where departmentid in (";
			for (int id : departmentidList) {
				sql += id + ",";
			}
			sql = sql.substring(0, sql.length() - 1);
			sql += ");";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("school id: " + rs.getInt("schoolid")
						+ " department id: " + rs.getInt("departmentid")
						+ " name: " + rs.getString("departmentname"));
			}

			System.out.println("major data :");
			sql = "select * from tmajor where majorid in (";
			for (int id : majoridList) {
				sql += id + ",";
			}
			sql = sql.substring(0, sql.length() - 1);
			sql += ");";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("school id: " + rs.getInt("schoolid")
						+ " department id: " + rs.getInt("departmentid")
						+ " major id: " + rs.getInt("majorid") + " name: "
						+ rs.getString("majorname"));
			}

			System.out.println("cost time :"
					+ (System.currentTimeMillis() - t1) + "ms!");
		} catch (Exception e) {
			System.out.print("get data error!");
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MySQL_JDBC_TEST m = new MySQL_JDBC_TEST();
		System.out.println("Recommand!");
		m.recommand(610, 1, 2013);
		// System.out.println("Search!");
		// m.search();
	}
}
