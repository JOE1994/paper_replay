import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.lang.StringBuilder;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class parse_data {

	Map<String, Integer> vocab_checker; // vocab MAP
	BufferedWriter init_topic_writer, topic_writer;
	java.sql.Connection con;

	// Constructor
	public parse_data() throws SQLException {
		// used to save doc frequency for each term.
		vocab_checker = new HashMap<String, Integer>();
		con = DriverManager
				.getConnection(
						"jdbc:mysql://localhost:3306/lab_second?autoReconnect=true&useSSL=false",
						"root", "2030kimm!");
	}

	public void connect2txt() {
		try {
			init_topic_writer = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(
									"C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/topics_list.txt",
									true), "UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out
				.println("Connected to :\nC:/ProgramData/MySQL/MySQL Server 5.7/Uploads/topics_list.txt");

	}

	public void close_txt() {
		try {
			init_topic_writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out
				.println("Closed connection to :\nC:/ProgramData/MySQL/MySQL Server 5.7/Uploads/topics_list.txt");
	}

	public void connect2dat() {
		try {
			topic_writer = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(
									"C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/topics.dat",
									true), "UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out
				.println("Connected to :\nC:/ProgramData/MySQL/MySQL Server 5.7/Uploads/topics.dat");
	}

	public void close_dat() {
		try {
			topic_writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out
				.println("Closed Connection to:\nC:/ProgramData/MySQL/MySQL Server 5.7/Uploads/topics.dat");
	}

	// count vocab and calculate document frequency.
	// + print topic_list of ODP
	DefaultHandler df_first = new DefaultHandler() {
		Boolean gate = false;
		String temp, topicid = "";
		String[] temp_array;
		Set<String> one_for_doc;

		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			if (qName.equals("Topic")) {
				topicid = attributes.getValue(0);
			} else if (qName.equals("d:Title")) {
				if (topicid.startsWith("Top/World")
						|| topicid.startsWith("Top/Regional"))
					gate = false;
				else {
					gate = true;
					one_for_doc = new HashSet<String>();
				}
			}
		}

		public void characters(char ch[], int start, int length)
				throws SAXException {
			if (gate) {
				temp = new String(ch, start, length);
				// split between 'space' or 'non-character'
				// change to LOWERCASE.

				temp_array = temp.toLowerCase().replaceAll("[\\W]", " ")
						.split(" ");
				for (String item : temp_array) {
					if (item.length() > 1 && Pattern.matches("\\D+", item)
							&& !one_for_doc.contains(item)) {
						if (vocab_checker.containsKey(item))
							vocab_checker
									.put(item, vocab_checker.get(item) + 1);
						else
							vocab_checker.put(item, 1);
						one_for_doc.add(item);
					}
				}
			}
		}

		public void endElement(String uri, String localName, String qName)
				throws SAXException {
			if (qName.equals("d:Description")) {
				gate = false;
				one_for_doc = null;
			}
		}
	};

	// count words in each topic & get doc_vector
	DefaultHandler df_second = new DefaultHandler() {

		boolean getready = false, topic_gate = false;
		String topicid = null, old_topicid = "", new_topicid = "";
		StringBuilder sum_content, cur_topic_jason;
		int linkcount = 0, followcount;
		JSONObject obj;

		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			if (qName.equals("Topic")) {
				linkcount = 0;
				followcount = 0;
				topicid = attributes.getValue(0);
				sum_content = null;
				if (topicid.startsWith("Top/World")
						|| topicid.startsWith("Top/Regional"))
					getready = false;
				else {
					getready = true;
					// Remove "Top/"
					topicid = topicid.replace("Top/", "");
					// Remove meaningless topic nodes
					Pattern p = Pattern.compile("/\\w/"); // A word character:
					// [a-zA-Z_0-9]
					Matcher m = p.matcher(topicid);
					while (m.find())
						topicid = topicid.substring(0, m.start())
								+ topicid.substring(m.end() - 1);
					topicid = topicid.replaceFirst("/\\w$", "");
					new_topicid = topicid;

					// new stringbuilder to concatenate topic text.
					sum_content = new StringBuilder(800);
				}

			} else if (qName.equals("d:Title")) {
				if (getready)
					topic_gate = true;
			} else if (qName.equals("link"))
				linkcount += 1;
		}

		public void characters(char ch[], int start, int length)
				throws SAXException {
			if (topic_gate) {
				// append to current StringBuilder
				sum_content.append(new String(ch, start, length));
				sum_content.append(" ");
			}
		}

		public void endElement(String uri, String localName, String qName)
				throws SAXException {
			if (qName.equals("d:Description") && topic_gate) {
				topic_gate = false;
				followcount += 1;
				// texts in current topic all gathered,
				// count term frequency in this topic.
				Map<String, Integer> cur_topic_vector;
				if (linkcount == followcount) {
					String topic_sum[] = sum_content.toString().toLowerCase()
							.replaceAll("[\\W]", " ").split(" ");
					cur_topic_vector = new HashMap<String, Integer>();
					for (String item : topic_sum) {
						if (vocab_checker.containsKey(item)) {
							if (cur_topic_vector.containsKey(item))
								cur_topic_vector.put(item,
										cur_topic_vector.get(item) + 1);
							else
								cur_topic_vector.put(item, 1);
						}
					}
					// Build String to write to.txt
					// (including JSON string.)
					cur_topic_jason = new StringBuilder(800);

					cur_topic_jason.append(topicid);
					cur_topic_jason.append('\t');
					int depth;
					if (topicid.equals(""))
						depth = 0;
					else {
						depth = 1;
						Matcher m = Pattern.compile("/").matcher(topicid);
						while (m.find())
							depth += 1;
					}
					cur_topic_jason.append(depth);
					cur_topic_jason.append('\t');

					obj = new JSONObject();
					for (Map.Entry<String, Integer> entry : cur_topic_vector
							.entrySet()) {
						// calculate tf-idf

						obj.put(entry.getKey(),
								(float) entry.getValue() / followcount
										/ vocab_checker.get(entry.getKey()));
					}
					cur_topic_jason.append(obj.toJSONString());
					cur_topic_jason.append('\n'); // end of JSON string

					// Write row info to .dat file.
					try {
						// write topic info to .txt
						init_topic_writer.write(topicid);
						init_topic_writer.flush();
						init_topic_writer.newLine();

						// write topic info to .dat
						topic_writer.write(cur_topic_jason.toString());
						topic_writer.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	};

	// insert all row data in .txt file.
	public void in2_DB() throws SQLException {
		java.sql.Statement st = null;
		try {
			con = DriverManager
					.getConnection(
							"jdbc:mysql://localhost:3306/lab_second?autoReconnect=true&useSSL=false",
							"root", "2030kimm!");
			st = con.createStatement();
			String topics_load = "LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/topics.dat' INTO TABLE topics LINES TERMINATED BY '\n' (topic_id, depth, vector_json);";
			st.executeUpdate(topics_load);
			System.out.println("Finished : topics_load applied to DB\n");

			// add kids_and_teens
			st.executeUpdate("INSERT INTO topics(topic_id, depth, vector_json) VALUES ('Kids_and_Teens', 1, '{}');");
			System.out.println("INSERT 'kids_and_teens' finished.\n");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (st != null)
				st.close();
		}
	}

	public void edit_leafDB() throws SQLException {
		java.sql.Statement stmt = null;
		java.sql.Statement stmt2 = null;
		try {
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			stmt2 = con.createStatement();
			for (int depth = 2; depth <= 13; depth++) {
				ResultSet one = stmt
						.executeQuery("SELECT topic_id, the_key FROM topics WHERE depth = "
								+ depth + ";");
				while (one.next()) {
					String old_topic_id = one.getString("topic_id");
					ResultSet two = stmt2
							.executeQuery("SELECT the_key FROM topics WHERE topic_id LIKE '"
									+ old_topic_id.replace("'", "\\'") + "/%';");
					if (two.next() == false) { // leaf found.
						one.updateString("topic_id",
								old_topic_id.replaceFirst("/.*$", ""));
						one.updateRow();
					}
				}
			}
			System.out.println("--- edit_leafDB --- END!");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null)
				stmt.close();
			if (stmt2 != null)
				stmt2.close();
		}
	}

	public void calc_subtopic_num() throws SQLException {
		java.sql.Statement stmt = null;
		java.sql.Statement stmt2 = null;
		try {
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			stmt2 = con.createStatement();
			ResultSet one = stmt
					.executeQuery("SELECT topic_id, subtopic_num, the_key FROM topics;");
			while (one.next()) {
				ResultSet two = stmt2
						.executeQuery("SELECT COUNT(topic_id) AS NUM FROM (SELECT topic_id, the_key FROM topics WHERE topic_id LIKE '"
								+ one.getString("topic_id").replace("'", "\\'")
								+ "/%') AS JK;");
				if (two.next()) {
					one.updateInt("subtopic_num", two.getInt("NUM"));
					one.updateRow();
				}
			}
			System.out.println("--- calc_subtopic_num --- END!");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null)
				stmt.close();
			if (stmt2 != null)
				stmt2.close();
		}
	}

	public void getXml(String xml_path, int whichone) throws IOException {
		try {
			// obtain and configure a SAX based parser
			SAXParserFactory saxParserfactory = SAXParserFactory.newInstance();

			// obtain object for SAX parser
			SAXParser saxParser = saxParserfactory.newSAXParser();
			if (whichone == 0)
				saxParser.parse(new BufferedInputStream(new FileInputStream(
						xml_path)), df_first);
			else if (whichone == 1)
				saxParser.parse(new BufferedInputStream(new FileInputStream(
						xml_path)), df_second);
			else
				System.exit(0);
			if (whichone == 0 || whichone == 1)
				System.out.println("Finished parsing->\n" + xml_path + "\n"
						+ "mode : " + whichone + "\n");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
	}

	public void print_vocab_info() {
		System.out.println("VOCAB SET size : " + vocab_checker.size());
	}
}
