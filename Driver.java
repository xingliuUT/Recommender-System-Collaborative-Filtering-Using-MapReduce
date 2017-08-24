
public class Driver {
	public static void main(String[] args) throws Exception {
		
		preProcessByUser preProcessByUser = new preProcessByUser();
		preProcessByItem preProcessByItem = new preProcessByItem();
		preProcessBaseline preProcessBaseline = new preProcessBaseline();
		normalizeRating normalizeRating = new normalizeRating();
		cooccurance cooccurance = new cooccurance();
		similarity similarity = new similarity();
		
		String rawInput = args[0];
		String byUserOutputDir = args[1];
		String byItemOutputDir = args[2];
		String baselineOutputDir = args[3];
		String normalizeOutputDir = args[4];
		String cooccuranceOutputDir = args[5];
		String similarityOutputDir = args[6];
		
		String[] path1 = {rawInput, byUserOutputDir};
		String[] path2 = {rawInput, byItemOutputDir};
		String[] path3 = {byItemOutputDir, baselineOutputDir};
		String[] path4 = {baselineOutputDir, byItemOutputDir, byUserOutputDir, normalizeOutputDir};
		String[] path5 = {byUserOutputDir, cooccuranceOutputDir};
		String[] path6 = {cooccuranceOutputDir, similarityOutputDir};
		
		preProcessByUser.main(path1);
		preProcessByItem.main(path2);
		preProcessBaseline.main(path3);
		normalizeRating.main(path4);
		cooccurance.main(path5);
		similarity.main(path6);
	}

}
