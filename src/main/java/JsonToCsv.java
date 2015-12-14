import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JsonToCsv {

	public static class JsonToCsvMapper
			extends Mapper<Object, Text, Text, NullWritable>{
		private static Text output = new Text();
		private static JSONParser parser = new JSONParser();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				for(String result : extractData(value.toString())) {
					output.set(result);
					context.write(output, NullWritable.get());
				}
			} catch (Exception e){
				e.printStackTrace();
			}
		}

		private List<String> extractData(String value) throws ParseException {
			List<String> results = new ArrayList<String>();
			JSONObject obj = (JSONObject) parser.parse(value);
			JSONObject result =  (JSONObject) obj.get("result");
			JSONArray players =  (JSONArray) result.get("players");

			for(Object player : players) {
				JSONObject _player = (JSONObject) player;
				long heroId = (Long) _player.get("hero_id");
				long item_0 =(Long) _player.get("item_0");
				long item_1 =(Long) _player.get("item_1");
				long item_2 =(Long) _player.get("item_2");
				long item_3 =(Long) _player.get("item_3");
				long item_4 =(Long) _player.get("item_4");
				long item_5 =(Long) _player.get("item_5");
				long deaths = (Long) _player.get("deaths");
				if (deaths == 0) {
					deaths = 1;
				}

				if (heroId == 0 || item_0 == 0 || item_1 == 0 ||
						item_2 == 0 || item_3 == 0 || item_4 == 0 || item_5 == 0) {
					continue;
				}

				ArrayList<Long> itemSet = new ArrayList<Long>();
				itemSet.add(item_0);
				itemSet.add(item_1);
				itemSet.add(item_2);
				itemSet.add(item_3);
				itemSet.add(item_4);
				itemSet.add(item_5);
				Collections.sort(itemSet);

				results.add(String.format("%s,%s,%s,%s", _player.get("hero_id"),
						Joiner.on('+').join(itemSet), _player.get("kills"), deaths));
			}
			return results;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: json-to-csv <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "json-to-csv");
		job.setJarByClass(JsonToCsv.class);
		job.setMapperClass(JsonToCsvMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}