package teamformer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import model.Player;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;

public class TeamFormer {

	public static class PlayerMapper extends Mapper<Object, Text, Text, Text> {

		private static CSVParser parser = new CSVParser();

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String[] tokens = parser.parseLine(value.toString());
			int id = Integer.parseInt(tokens[0]);

			context.write(new Text(String.valueOf(id)), new Text(tokens[1]
					+ "," + tokens[2] + "," + tokens[3]));
		}

	}

	public static class PlayerReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			long killCount = 0L;
			long deathCount = 1L;

			for (Text match : values) {
				String[] killDeath = match.toString().split(",");
				killCount = killCount + Long.parseLong(killDeath[0]) + Long.parseLong(killDeath[1]);
				deathCount = deathCount + Long.parseLong(killDeath[2]);
			}

			context.write(
					key,
					new Text(String.format("%.10f", (double) killCount
							/ deathCount)));
		}

	}

	public static class WinnerMapper extends Mapper<Object, Text, Text, Text> {

		private Queue<Player> localWinners;

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			while (!localWinners.isEmpty()) {
				Player p = localWinners.remove();

				context.write(
						new Text("Dummy"),
						new Text(String.format("%d, %.10f", p.getId(),
								p.getScore())));
			}

		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			localWinners = new PriorityQueue<Player>();

		}

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length == 2) {
				double score = Double.parseDouble(tokens[1]);
				Player p = new Player();
				p.setId(Long.parseLong(tokens[0]));
				p.setScore(score);

				if (localWinners.size() < 10) {
					localWinners.add(p);
				} else {
					double lastScore = localWinners.peek().getScore();
					if (lastScore <= score) {
						localWinners.remove();
						localWinners.add(p);
					}
				}
			}

		}

	}

	public static class WinnerReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<Player> list = new ArrayList<>();
			for (Text txt : values) {
				String[] tokens = txt.toString().split(",");
				Player p = new Player();
				p.setId(Long.parseLong(tokens[0]));
				p.setScore(Double.parseDouble(tokens[1]));
				list.add(p);
			}
			Collections.sort(list, Collections.reverseOrder());
			for (int i = 0; i < 10; i++) {
				Player p = list.get(i);
				context.write(new Text(String.valueOf(p.getId())),
						new Text(String.valueOf(p.getScore())));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		String[] otherArgs = parser.getRemainingArgs();

		if (args.length != 3) {
			System.out.println("Usage TeamFormer <input> <output>");
			System.exit(2);
		}

		Job job = createJob(conf, "1st job", TeamFormer.class,
		PlayerMapper.class, PlayerReducer.class, 10, Text.class,
		Text.class, otherArgs[0], otherArgs[1]);
		job.waitForCompletion(true);

		Job job2 = createJob(conf, "2nd job", TeamFormer.class,
				WinnerMapper.class, WinnerReducer.class, 1, Text.class,
				Text.class, otherArgs[1], otherArgs[2]);

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}

	@SuppressWarnings({ "rawtypes", "deprecation" })
	private static Job createJob(Configuration conf, String jobName,
			Class jarClass, Class<? extends Mapper> mapper,
			Class<? extends Reducer> reducer, int numReduceTask,
			Class outputKey, Class outValue, String inputPath, String outputPath)
			throws IOException {

		Job job = new Job(conf, jobName);
		job.setJarByClass(jarClass);
		job.setMapperClass(mapper);
		job.setReducerClass(reducer);
		job.setNumReduceTasks(numReduceTask);
		job.setOutputKeyClass(outputKey);
		job.setOutputValueClass(outValue);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job;
	}

}