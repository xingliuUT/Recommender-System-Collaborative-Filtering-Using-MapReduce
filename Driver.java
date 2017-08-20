
public class Driver {
	public static void main(String[] args) throws Exception {
		
		preProcessByUser preProcessByUser = new preProcessByUser();
		
		String rawInput = args[0];
		String byUserOutputDir = args[1];
		
		String[] path1 = {rawInput, byUserOutputDir};
		
		preProcessByUser.main(path1);
	}

}
