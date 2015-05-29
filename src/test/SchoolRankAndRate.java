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

public class SchoolRankAndRate {
	Statement stmt;
	String filePath;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SchoolRankAndRate s = new SchoolRankAndRate("schoolrank.txt");
		s.process();

	}

	public SchoolRankAndRate(String path) {
		filePath = path;
		try {
			Class.forName("com.mysql.jdbc.Driver"); // ����MYSQL JDBC��������
			// Class.forName("org.gjt.mm.mysql.Driver");
			System.out.println("Success loading Mysql Driver!");
		} catch (Exception e) {
			System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {
			Connection connect = DriverManager
					.getConnection(
							"jdbc:mysql://localhost:3306/db_recommand?useUnicode=true&characterEncoding=utf-8",
							"root", "");
			// ����URLΪ jdbc:mysql//��������ַ/���ݿ��� �������2�������ֱ��ǵ�½�û���������
			System.out.println("Success connect Mysql server!");
			stmt = connect.createStatement();
		} catch (Exception e) {
			System.out.print("get data error!");
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
				while ((line = bufferedReader.readLine()) != null) {
					// System.out.println(line);
					int schoolType = 0;
					String[] schoolTypeName = { "��", "�ۺ�", "�ƾ�", "����", "ũ��",
							"ʦ��", "ҽҩ", "����", "����" };
					String schoolName = line.split("\t")[1].trim();
					int rank = Integer.parseInt(line.split("\t")[0]);
					String localityp = line.split("\t")[3];
					double rate = Double.parseDouble(line.split("\t")[5]);
					for (int i = 0; i < schoolTypeName.length; i++) {
						if (line.split("\t")[2].equals(schoolTypeName[i])) {
							schoolType = i + 1;
							break;
						}
					}
					String sql;
					try {
						sql = "update tschool set schooltype = "
								+ schoolType
								+ " , schoolrank = "
								+ rank
								+ " , schoolrrate = "
								+ rate
								+ " , localityp = (select class_id from tlocality where class_name like \""
								+ localityp
								+ "\" and class_type = 1) where schoolname like  \""
								+ schoolName + "\";";
						System.out.println(sql);
						stmt.executeUpdate(sql);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
				bufferedReader.close();
			} catch (FileNotFoundException e) {
				// TODO �Զ����ɵ� catch ��
				e.printStackTrace();
			} catch (IOException e) {
				// TODO �Զ����ɵ� catch ��
				e.printStackTrace();
			}
		}
		System.out.println("cost time :" + (System.currentTimeMillis() - t1)
				+ "ms!");

	}
}
