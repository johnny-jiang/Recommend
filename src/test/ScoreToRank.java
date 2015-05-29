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

public class ScoreToRank {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		insertScoreToRank("2011��ʷ.txt", 0, 2011);
		insertScoreToRank("2011��.txt", 1, 2011);
		insertScoreToRank("2012��ʷ.txt", 0, 2012);
		insertScoreToRank("2012��.txt", 1, 2012);
		insertScoreToRank("2013��ʷ.txt", 0, 2013);
		insertScoreToRank("2013��.txt", 1, 2013);
		insertScoreToRank("2014��ʷ.txt", 0, 2014);
		insertScoreToRank("2014��.txt", 1, 2014);
	}

	public static void insertScoreToRank(String filePath, int type, int year) {
		try {
			Class.forName("com.mysql.jdbc.Driver"); // ����MYSQL JDBC��������
			// Class.forName("org.gjt.mm.mysql.Driver");
			System.out.println("Success loading Mysql Driver!");
		} catch (Exception e) {
			System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {
			String sql;
			ResultSet rs;
			Statement stmt;
			int score;
			int topRank, bottomRank;
			int[] rank = new int[11];
			rank[0] = 0;
			Connection connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/db_recommand", "root", "");
			// ����URLΪ jdbc:mysql//��������ַ/���ݿ��� �������2�������ֱ��ǵ�½�û���������
			System.out.println("Success connect Mysql server!");
			stmt = connect.createStatement();
			long t1 = System.currentTimeMillis();
			File pDatFile = new File(filePath);
			if (pDatFile.exists() == true) {
				FileReader reader;
				try {
					reader = new FileReader(filePath);
					BufferedReader bufferedReader = new BufferedReader(reader);
					String line = null;
					while ((line = bufferedReader.readLine()) != null) {
						System.out.println(line);
						score = Integer.parseInt(line.split(",")[0]);
						for (int i = 1; i < 11; i++) {
							rank[i] = Integer.parseInt(line.split(",")[i]);
						}
						for (int i = 0; i < 10; i++) {
							System.out.printf(
									"score: %d topRank: %d bottomRank: %d\n",
									score + i, rank[9 - i], rank[10 - i]);
							sql = "insert into tscoretorank values(0,"
									+ (score + i) + "," + rank[9 - i] + ","
									+ rank[10 - i] + ","+year+"," + type + ");";
							 System.out.println(sql);
							stmt.executeUpdate(sql);
							if (stmt.getUpdateCount() > 0)
								System.out
										.println("insert scuess!\nupdate count : "
												+ stmt.getUpdateCount());
						}
						rank[0] = rank[10];
					}
					bufferedReader.close();
				} catch (FileNotFoundException e) {
					// TODO �Զ����ɵ� catch ��
					e.printStackTrace();
				} catch (IOException e) {
					// TODO �Զ����ɵ� catch ��
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
