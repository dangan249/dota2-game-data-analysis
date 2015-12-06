package csvgenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class InputFormatter {
	public static void main(String[] args) throws Exception {
		final PrintWriter writer = new PrintWriter("data.csv");
		
		String inputDirectory = "DotaData";
		Files.walkFileTree(Paths.get(inputDirectory), new FileVisitor<Path>() {
				
			@Override
			public FileVisitResult preVisitDirectory(Path dir,
					BasicFileAttributes attrs) throws IOException {
				return FileVisitResult.CONTINUE;
			}
 
			@Override
			public FileVisitResult visitFile(Path file,
					BasicFileAttributes attrs) throws IOException {
				                        System.out.println(file);
				parseFile(file, writer);
				
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc)
					throws IOException {
				
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc)
					throws IOException {
				return FileVisitResult.CONTINUE;
			}
		});
		
		writer.close();		
	}
	
	private static void parseFile(Path file, PrintWriter writer) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file.toFile()));
		String line;
		while((line = reader.readLine())!= null){
			
			try {
				writer.println(extractData(line));
			} catch (Exception e) {
				e.printStackTrace();
                            System.out.println(line);
			}
			
		}
		
		reader.close();		
	}
		
	private static JSONParser parser = new JSONParser();	

	private static String extractData(String value) throws Exception {
		JSONObject obj = (JSONObject) parser.parse(value.toString());
		JSONObject result =  (JSONObject) obj.get("result");
		JSONArray players =  (JSONArray) result.get("players");
                StringBuilder builder = new StringBuilder();
                for(Object player : players) {
                    JSONObject _player = (JSONObject) player;
                    String hero = getPlayers(_player);                    
                    builder.append(hero);
                    builder.append("\n");
                }
                builder.deleteCharAt(builder.length()-1);
		return builder.toString();
	}
			       
        private static String getPlayers(JSONObject _player){
            StringBuilder strBuilder = new StringBuilder();                        
            strBuilder.append(_player.get("hero_id").toString());
            strBuilder.append(",");
            strBuilder.append(_player.get("kills").toString());
            strBuilder.append(",");
            strBuilder.append(_player.get("assists").toString());
            strBuilder.append(",");
            strBuilder.append(_player.get("deaths").toString());	                      
            return strBuilder.toString();        	
        }
}