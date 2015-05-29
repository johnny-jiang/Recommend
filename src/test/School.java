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

public class School {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		insertSchool("学校信息.txt");
	}

	public static void insertSchool(String filePath) {
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
			Connection connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/db_recommand?useUnicode=true&characterEncoding=utf-8", "root", "");
			// 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
			System.out.println("Success connect Mysql server!");
			stmt = connect.createStatement();
			long t1 = System.currentTimeMillis();

			String generalID;
			String name;
			String local;
			int a, b;
			int type;
			int localityp = 0, localityc = 0;
			File pDatFile = new File(filePath);
			if (pDatFile.exists() == true) {
				FileReader reader;
				try {
					reader = new FileReader(filePath);
					BufferedReader bufferedReader = new BufferedReader(reader);
					String line = null;
					while ((line = bufferedReader.readLine()) != null) {
						// System.out.println(line);
						generalID = line.split(",")[0];
						name = line.split(",")[1];
						local = line.split(",")[2];
						a = Integer.parseInt(line.split(",")[3]);
						b = Integer.parseInt(line.split(",")[4]);
						if (a == 0 && b == 1)
							type = 1;
						else if (a == 1 && b == 0)
							type = 0;
						else
							type = 2;
						sql = "select * from tlocality where class_name like '"
								+ local + "%' and class_type=2;";
						System.out.println(sql);
						rs = stmt.executeQuery(sql);
						if (!rs.next()) {
							System.out.println(4);
							sql = "select * from tlocality where class_name like '"
									+ local + "%' and class_type=3;";
							System.out.println(sql);
							rs = stmt.executeQuery(sql);
							rs.next();
						}
						localityc = rs.getInt("class_id");
						localityp = rs.getInt("class_parent_id");

						sql = "insert into tschool(schoolid,generalID,schoolname,localityp,localityc,schooltype) "
								+ "values(0,\""
								+ generalID
								+ "\",\""
								+ name
								+ "\","
								+ localityp
								+ ","
								+ localityc
								+ ","
								+ type + ")";
						System.out.println(sql);
						stmt.executeUpdate(sql);
						if (stmt.getUpdateCount() > 0)
							System.out
									.println("insert scuess!\nupdate count : "
											+ stmt.getUpdateCount());
					}
					sql="insert into tdepartment(departmentid,schoolid,departmentname) select 0,schoolid,schoolname from tschool;";
					System.out.println(sql);
					stmt.executeUpdate(sql);
					if (stmt.getUpdateCount() > 0)
						System.out
								.println("insert scuess!\nupdate count : "
										+ stmt.getUpdateCount());
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
