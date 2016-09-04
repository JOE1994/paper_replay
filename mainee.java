import java.io.IOException;
import java.sql.SQLException;

public class mainee {
	public static void main(String[] args) throws IOException, SQLException {

		final int check_wordpool = 0;
		final int second_phase = 1;

		// check vocabulary info in document pool,
		// get document frequency + print original topics_list
		parse_data checkcheck = new parse_data();
		checkcheck.getXml(
				"C:/Users/Kim/Desktop/2016여름/content.rdf.u8/content.rdf.u8",
				check_wordpool);
		// checkcheck.getXml("C:/Users/Kim/Desktop/2016여름/ad-content.rdf.u8");
		checkcheck.getXml("C:/Users/Kim/Desktop/2016여름/kt-content.rdf.u8",
				check_wordpool); 
		checkcheck.print_vocab_info();

		for (String key : checkcheck.vocab_checker.keySet())
			System.out
					.println(key + " -> " + checkcheck.vocab_checker.get(key));
		System.out.println("\n");
		
		// calculate topic vector while parsing.
		checkcheck.connect2txt();	// open topics_list.txt
		checkcheck.connect2dat();	// open topics.dat
		checkcheck.getXml("C:/Users/Kim/Desktop/2016여름/content.rdf.u8/content.rdf.u8", second_phase);
		checkcheck.getXml("C:/Users/Kim/Desktop/2016여름/kt-content.rdf.u8",
				second_phase);
		checkcheck.close_txt();		// close topics_list.txt
		checkcheck.close_dat();	// close topics.dat
		
		// save each vector info in DB (JSON format)
		checkcheck.in2_DB();
		
		// remove leaf topics.
		// checkcheck.edit_leafDB();
		
		// calculate subtopic numbers for each row.
		// checkcheck.calc_subtopic_num();
		
		// close connection to DB
		checkcheck.con.close();
	}
}