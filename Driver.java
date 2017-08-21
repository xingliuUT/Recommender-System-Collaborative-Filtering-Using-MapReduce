
public class Driver {
	public static void main(String[] args) throws Exception {
		
		preProcessByUser preProcessByUser = new preProcessByUser();
		preProcessByItem preProcessByItem = new preProcessByItem();
		
		String rawInput = args[0];
		String byUserOutputDir = args[1];
		String byItemOutputDir = args[2];
		
		String[] path1 = {rawInput, byUserOutputDir};
		String[] path2 = {rawInput, byItemOutputDir};
		
		preProcessByUser.main(path1);
		preProcessByItem.main(path2);
	}

}
