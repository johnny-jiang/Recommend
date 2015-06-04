package test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GenerateRecord {

	void recommand(int score, int risk, int year, int type, int enrollType) {
		try {
			Class.forName("com.mysql.jdbc.Driver"); // 加载MYSQL JDBC驱动程序
			// Class.forName("org.gjt.mm.mysql.Driver");
			// System.out.println("Success loading Mysql Driver!");
		} catch (Exception e) {
			// System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {
			String sql;
			int localityp = 2;
			int localityc = 0;
			int majortype = type;
			int majorclass = 0;
			int majorenrolltype = enrollType;
			int is211 = 0;
			int is985 = 0;
			int generalcode = 0;
			double safeDown = -3, safeUp = 5;
			double normalDown = -5, normalUp = 10;
			double riskDown = -10, riskUp = 15;
			int c20110 = 39794;
			int c20111 = 106748;
			int c20120 = 40189;
			int c20121 = 101734;
			int c20130 = 39056;
			int c20131 = 97106;
			int c20140 = 39349;
			int c20141 = 94320;
			int opc0 = 0, opc1 = 0;
			if (year == 2011) {
				opc0 = c20110;
				opc1 = c20111;
			}
			if (year == 2012) {
				opc0 = c20120;
				opc1 = c20121;
			}
			if (year == 2013) {
				opc0 = c20130;
				opc1 = c20131;
			}
			ResultSet rs;
			Statement stmt;
			int ranktop, rankbottom;
			double top, bottom;
			Connection connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/recommend_test", "root", "");
			// 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
			// System.out.println("Success connect Mysql server!");
			stmt = connect.createStatement();
			// long t1 = System.currentTimeMillis();
			sql = "select count from vrecorded where year=" + year
					+ " and risk = " + risk + " and score = " + score
					+ " and type = " + majortype + ";";
			// System.out.println(sql);
			rs = stmt.executeQuery(sql);
			rs.next();
			if (!rs.first()) {
				// System.out.println("dataset is null , insert data.");
				sql = "select ranktop,rankbottom from tscoretorank where score="
						+ score
						+ " and year="
						+ year
						+ " and type="
						+ majortype + ";";
				// System.out.println(sql);
				rs = stmt.executeQuery(sql);
				rs.next();
				// System.out.println("score: " + score);
				ranktop = rs.getInt("ranktop");
				rankbottom = rs.getInt("rankbottom");
				if (type == 0) {
					ranktop = ranktop * (opc0 / c20140);
					rankbottom = rankbottom * (opc0 / c20140);
				} else {
					ranktop = ranktop * (opc1 / c20141);
					rankbottom = rankbottom * (opc1 / c20141);
				}
				// System.out.println("ranktop:" + ranktop + "\nrankbottom:"
				// + rankbottom);
				sql = "select score from tscoretorank where year = " + year
						+ " and type = " + majortype + " and ranktop <="
						+ ranktop + " and rankbottom > " + ranktop + ";";
				rs = stmt.executeQuery(sql);
				rs.next();
				top = rs.getDouble("score");
				// System.out.println("top score:" + top);
				sql = "select score from tscoretorank where year = " + year
						+ " and type = " + majortype + " and  ranktop <"
						+ rankbottom + 1 + " and rankbottom > " + rankbottom
						+ ";";
				// System.out.println(sql);
				rs = stmt.executeQuery(sql);
				rs.next();
				bottom = rs.getDouble("score");
				// System.out.println("bottom score:" + bottom);

				if (type == 1) {
					if (top >= 650 && top < 700) {
						safeDown = -10;
						safeUp = 10;
						normalDown = -15;
						normalUp = 10;
						riskDown = -20;
						riskUp = 15;
					} else if (top >= 700) {
						safeDown = -20;
						safeUp = 20;
						normalDown = -30;
						normalUp = 30;
						riskDown = -40;
						riskUp = 40;
					} else if (top >= 500 && top < 650) {
						safeDown = -5;
						safeUp = 5;
						normalDown = -8;
						normalUp = 8;
						riskDown = -10;
						riskUp = 10;
					} else {
						safeDown = -5;
						safeUp = 5;
						normalDown = -10;
						normalUp = 10;
						riskDown = -15;
						riskUp = 15;
					}
				} else {
					if (top >= 650 && top < 700) {
						safeDown = -25;
						safeUp = 15;
						normalDown = -40;
						normalUp = 20;
						riskDown = -60;
						riskUp = 25;
					} else if (top >= 700) {
						safeDown = -30;
						safeUp = 30;
						normalDown = -40;
						normalUp = 40;
						riskDown = -60;
						riskUp = 60;
					} else {
						safeDown = -10;
						safeUp = 10;
						normalDown = -15;
						normalUp = 15;
						riskDown = -25;
						riskUp = 25;
					}
				}
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
							+ (double) (bottom + safeDown)
							+ " and "
							+ (double) (top + safeUp)
							+ " and topscore+bottomscore between "
							+ (double) 2
							* (bottom + safeDown)
							+ " and "
							+ (double) 2
							* (bottom + safeUp)
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
							+ (double) (top + normalUp)
							+ " and topscore*3/4 + bottomscore/4 between "
							+ (double) (bottom + normalDown)
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
							+ (double) (top + riskUp)
							+ " and topscore + bottomscore between "
							+ (double) 2
							* (top + riskDown)
							+ " and "
							+ (double) 2
							* (top + riskUp)
							+ " and majorid in (select majorid from tmajor where majortype = "
							+ majortype + ");";
					break;
				}
				// System.out.println(sql);
				stmt.executeUpdate(sql);
				if (stmt.getUpdateCount() > 0)
					System.out.println("insert scuess! " + "year: " + year
							+ " score: " + score + " risk: " + risk + ","
							+ majortype + " update count : "
							+ stmt.getUpdateCount());
			} else {
				// System.out.println("data set exists.");
				// int count = rs.getInt("count");
				// System.out.println("data count: " + count);
			}
			// sql =
			// "select * from tmajorscoreline where id in (select resultid from trecord where year = "
			// + year
			// + " and risk = "
			// + risk
			// + " and score = "
			// + score
			// + " and type = "
			// + majortype
			// +
			// ") and majorid in(select majorid from tmajor where majortype = "
			// + majortype
			// + " and majorclass = "
			// + majorclass
			// + " and majorenrolltype = "
			// + majorenrolltype
			// + " and generalcode =" + generalcode + " );";
			// System.out.println(sql);
			//
			// ResultSet scoreLineRS = stmt.executeQuery(sql);
			// List<Integer> majoridList = new ArrayList<Integer>();
			// List<Integer> schoolidList = new ArrayList<Integer>();
			// List<Integer> departmentidList = new ArrayList<Integer>();
			// int majorid, schoolid, departmentid;
			// System.out.println("major score line :");
			// while (scoreLineRS.next()) {
			// majorid = scoreLineRS.getInt("majorid");
			// departmentid = scoreLineRS.getInt("departmentid");
			// schoolid = scoreLineRS.getInt("schoolid");
			// if (!majoridList.contains(majorid))
			// majoridList.add(majorid);
			// if (!departmentidList.contains(departmentid))
			// departmentidList.add(departmentid);
			// if (!schoolidList.contains(schoolid))
			// schoolidList.add(schoolid);
			// System.out.println("school id: "
			// + scoreLineRS.getInt("schoolid") + " department id: "
			// + scoreLineRS.getInt("departmentid") + " id: "
			// + scoreLineRS.getInt("id") + " schooling: "
			// + scoreLineRS.getDouble("schooling") + " topScore:"
			// + scoreLineRS.getDouble("topscore") + " bottomScore:"
			// + scoreLineRS.getDouble("bottomscore"));
			// }
			//
			// System.out.println("school data :");
			// sql = "select * from tschool where schoolid in (";
			// for (int id : schoolidList) {
			// sql += id + ",";
			// }
			// sql = sql.substring(0, sql.length() - 1);
			// sql += ");";
			// rs = stmt.executeQuery(sql);
			// while (rs.next()) {
			// System.out.println("school id: " + rs.getInt("schoolid")
			// + " name: " + rs.getString("schoolname"));
			// }
			// rs.last();
			// System.out.println("school count: " + rs.getRow());
			//
			// System.out.println("department data :");
			// sql = "select * from tdepartment where departmentid in (";
			// for (int id : departmentidList) {
			// sql += id + ",";
			// }
			// sql = sql.substring(0, sql.length() - 1);
			// sql += ");";
			// rs = stmt.executeQuery(sql);
			// while (rs.next()) {
			// System.out.println("school id: " + rs.getInt("schoolid")
			// + " department id: " + rs.getInt("departmentid")
			// + " name: " + rs.getString("departmentname"));
			// }
			// rs.last();
			// System.out.println("department count: " + rs.getRow());
			//
			// System.out.println("major data :");
			// sql = "select * from tmajor where majorid in (";
			// for (int id : majoridList) {
			// sql += id + ",";
			// }
			// sql = sql.substring(0, sql.length() - 1);
			// sql += ");";
			// rs = stmt.executeQuery(sql);
			// while (rs.next()) {
			// System.out.println("school id: " + rs.getInt("schoolid")
			// + " department id: " + rs.getInt("departmentid")
			// + " major id: " + rs.getInt("majorid") + " name: "
			// + rs.getString("majorname"));
			// }
			// rs.last();
			// System.out.println("major count: " + rs.getRow());

			// System.out.println("cost time :"
			// + (System.currentTimeMillis() - t1) + "ms!");
		} catch (Exception e) {
			System.out.print("get data error!");
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GenerateRecord m = new GenerateRecord();
		System.out.println("Recommand!");

		long t1 = System.currentTimeMillis();
		for (int i = 0; i < 750; i++) {
			m.recommand(750 - i, 0, 2011, 0, 0);
			m.recommand(750 - i, 1, 2011, 0, 0);
			m.recommand(750 - i, 2, 2011, 0, 0);
			m.recommand(750 - i, 0, 2011, 1, 0);
			m.recommand(750 - i, 1, 2011, 1, 0);
			m.recommand(750 - i, 2, 2011, 1, 0);
		}
		System.out.println(" 2011 cost time :"
				+ (System.currentTimeMillis() - t1) + "ms!");

		t1 = System.currentTimeMillis();
		for (int i = 0; i < 750; i++) {
			m.recommand(750 - i, 0, 2012, 0, 0);
			m.recommand(750 - i, 1, 2012, 0, 0);
			m.recommand(750 - i, 2, 2012, 0, 0);
			m.recommand(750 - i, 0, 2012, 1, 0);
			m.recommand(750 - i, 1, 2012, 1, 0);
			m.recommand(750 - i, 2, 2012, 1, 0);
		}
		System.out.println(" 2012 cost time :"
				+ (System.currentTimeMillis() - t1) + "ms!");
		t1 = System.currentTimeMillis();
		for (int i = 0; i < 750; i++) {
			m.recommand(750 - i, 0, 2013, 0, 0);
			m.recommand(750 - i, 1, 2013, 0, 0);
			m.recommand(750 - i, 2, 2013, 0, 0);
			m.recommand(750 - i, 0, 2013, 1, 0);
			m.recommand(750 - i, 1, 2013, 1, 0);
			m.recommand(750 - i, 2, 2013, 1, 0);
		}
		System.out.println(" 2013 cost time :"
				+ (System.currentTimeMillis() - t1) + "ms!");
		t1 = System.currentTimeMillis();
		for (int i = 0; i < 750; i++) {
			m.recommand(750 - i, 0, 2014, 0, 0);
			m.recommand(750 - i, 1, 2014, 0, 0);
			m.recommand(750 - i, 2, 2014, 0, 0);
			m.recommand(750 - i, 0, 2014, 1, 0);
			m.recommand(750 - i, 1, 2014, 1, 0);
			m.recommand(750 - i, 2, 2014, 1, 0);
		}
		System.out.println(" 2014 cost time :"
				+ (System.currentTimeMillis() - t1) + "ms!");
		// m.recommand(750, 0, 2011, 1, 9);
		// m.recommand(750, 1, 2011, 1, 9);
		// m.recommand(750, 2, 2011, 1, 9);

		// System.out.println("Search!");
		// m.search();
	}
}
