package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Scoreline {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		insertScore("专业分数线.txt", 2013, 0, 1);
	}

	public static void insertScore(String filePath, int year, int type,
			int enrolltype) {
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
			Statement stmt;
			ResultSet rs;
			Connection connect = DriverManager
					.getConnection(
							"jdbc:mysql://localhost:3306/db_recommand?useUnicode=true&characterEncoding=utf-8",
							"root", "");
			// 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
			System.out.println("Success connect Mysql server!");
			stmt = connect.createStatement();
			long t1 = System.currentTimeMillis();
			String schoolname;
			int schoolid = 0;
			int departmentid = 0;
			File pDatFile = new File(filePath);
			if (pDatFile.exists() == true) {
				FileReader reader;
				try {
					reader = new FileReader(filePath);
					BufferedReader bufferedReader = new BufferedReader(reader);
					String line = null;
					while ((line = bufferedReader.readLine()) != null) {
						// System.out.println(line);
						if (line.split(",")[0].equals("0")
								&& !line.split(",")[1].equals("0")) {
							String majorname = line.split(",")[1];
							int majorid = 0;
							sql = "select majorid from tmajor where schoolid ="
									+ schoolid + " and departmentid = "
									+ departmentid + " and majorname like '"
									+ majorname + "';";
							System.out.println(sql);
							rs = stmt.executeQuery(sql);
							if (!rs.next()) {
								System.out.println("insert major data");
								sql = "insert into tmajor(schoolid,departmentid,majorid,majorname) values("
										+ schoolid
										+ ","
										+ departmentid
										+ ",0,'" + majorname + "');";
								System.out.println(sql);
								stmt.executeUpdate(sql);
								if (stmt.getUpdateCount() > 0)
									System.out
											.println("insert scuess!\nupdate count : "
													+ stmt.getUpdateCount());
								sql = "select majorid from tmajor where schoolid ="
										+ schoolid
										+ " and departmentid = "
										+ departmentid
										+ " and majorname like '"
										+ majorname
										+ "';";
								rs = stmt.executeQuery(sql);
								rs.next();
								majorid = rs.getInt("majorid");
							} else {
								majorid = rs.getInt("majorid");
							}
							System.out.println("majorid:" + majorid);

							double topscore = Double.parseDouble(line
									.split(",")[2]);
							double bottomscore = Double.parseDouble(line
									.split(",")[3]);
							double firsttopscore = Double.parseDouble(line
									.split(",")[4]);
							double firstbottomscore = Double.parseDouble(line
									.split(",")[5]);
							double paralleltopscore = 0;
							double parallelbottomscore = 0;
							double additionaltopscore = 0;
							double additionalbottomscore = 0;
							if (line.split(",").length == 8) {
								paralleltopscore = Double.parseDouble(line
										.split(",")[6]);
								parallelbottomscore = Double.parseDouble(line
										.split(",")[7]);
							} else if (line.split(",").length == 10) {
								paralleltopscore = Double.parseDouble(line
										.split(",")[6]);
								parallelbottomscore = Double.parseDouble(line
										.split(",")[7]);
								additionaltopscore = Double.parseDouble(line
										.split(",")[8]);
								additionalbottomscore = Double.parseDouble(line
										.split(",")[9]);
							}
							sql = "insert into tmajorscoreline(id,schoolid,departmentid,majorid,year,topscore,bottomscore,firsttopscore,firstbottomscore"
									+ ",paralleltopscore,parallelbottomscore,additionaltopscore,additionalbottomscore) values(0,"
									+ schoolid
									+ ","
									+ departmentid
									+ ","
									+ majorid
									+ ","
									+ year
									+ ","
									+ topscore
									+ ","
									+ bottomscore
									+ ","
									+ firsttopscore
									+ ","
									+ firstbottomscore
									+ ","
									+ paralleltopscore
									+ ","
									+ parallelbottomscore
									+ ","
									+ additionaltopscore
									+ ","
									+ additionalbottomscore + ");";
							System.out.println(sql);
							stmt.executeUpdate(sql);
							if (stmt.getUpdateCount() > 0)
								System.out
										.println("insert scuess!\nupdate count : "
												+ stmt.getUpdateCount());
						} else if (!line.split(",")[0].equals("0")
								&& line.split(",")[1].equals("0")) {
							schoolname = line.split(",")[0];
							int actuallyenroll = Integer.parseInt(line
									.split(",")[2]);
							double topscore = Double.parseDouble(line
									.split(",")[3]);
							double bottomscore = Double.parseDouble(line
									.split(",")[4]);

							int firstcount = Integer
									.parseInt(line.split(",")[5]);
							double firsttopscore = Double.parseDouble(line
									.split(",")[6]);
							double firstbottomscore = Double.parseDouble(line
									.split(",")[7]);
							int parallelcount = 0;
							double paralleltopscore = 0;
							double parallelbottomscore = 0;
							int additionalcount = 0;
							double additionaltopscore = 0;
							double additionalbottomscore = 0;
							if (actuallyenroll > firstcount) {
								parallelcount = Integer.parseInt(line
										.split(",")[8]);
								paralleltopscore = Double.parseDouble(line
										.split(",")[9]);
								parallelbottomscore = Double.parseDouble(line
										.split(",")[10]);
								if (parallelcount + firstcount < actuallyenroll) {
									additionalcount = Integer.parseInt(line
											.split(",")[11]);
									additionaltopscore = Double
											.parseDouble(line.split(",")[12]);
									additionalbottomscore = Double
											.parseDouble(line.split(",")[13]);
								}
							}
							sql = "select schoolid from tschool where schoolname like '"
									+ schoolname + "';";
							System.out.println(sql);
							rs = stmt.executeQuery(sql);
							rs.next();
							schoolid = rs.getInt("schoolid");
							sql = "select departmentid from tdepartment where schoolid ="
									+ schoolid + ";";
							System.out.println(sql);
							rs = stmt.executeQuery(sql);
							rs.next();
							departmentid = rs.getInt("departmentid");

							sql = "insert into tschoolscoreline values(0,"
									+ schoolid + "," + year + "," + type + ","
									+ enrolltype + "," + actuallyenroll + ","
									+ topscore + "," + bottomscore + ","
									+ firstcount + "," + firsttopscore + ","
									+ firstbottomscore + "," + parallelcount
									+ "," + paralleltopscore + ","
									+ parallelbottomscore + ","
									+ additionalcount + ","
									+ additionaltopscore + ","
									+ additionalbottomscore + ");";
							System.out.println(sql);
							stmt.executeUpdate(sql);
							if (stmt.getUpdateCount() > 0)
								System.out
										.println("insert scuess!\nupdate count : "
												+ stmt.getUpdateCount());
						} else if (line.split(",")[0].equals("0")
								&& line.split(",")[1].equals("0")) {
							continue;
						}
					}
					bufferedReader.close();
				} catch (FileNotFoundException e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				} catch (IOException e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}
			} else {
			}

			System.out.println("cost time :"
					+ (System.currentTimeMillis() - t1) + "ms!");

		} catch (Exception e) {
			System.out.print("get data error!");
			e.printStackTrace();
		}
	}
}
