
public class Driver {
	public static void main(String[] args) throws Exception {
		
		preProcessByUser preProcessByUser = new preProcessByUser();
		preProcessByItem preProcessByItem = new preProcessByItem();
		preProcessBaseline preProcessBaseline = new preProcessBaseline();
		normalizeRating normalizeRating = new normalizeRating();
		
		String rawInput = args[0];
		String byUserOutputDir = args[1];
		String byItemOutputDir = args[2];
		String baselineOutputDir = args[3];
		String normalizeOutputDir = args[4];
		
		String[] path1 = {rawInput, byUserOutputDir};
		String[] path2 = {rawInput, byItemOutputDir};
		String[] path3 = {byItemOutputDir, baselineOutputDir};
		String[] path4 = {baselineOutputDir, byItemOutputDir, byUserOutputDir, normalizeOutputDir};
		
		preProcessByUser.main(path1);
		preProcessByItem.main(path2);
		preProcessBaseline.main(path3);
		normalizeRating.main(path4);
	}

}
