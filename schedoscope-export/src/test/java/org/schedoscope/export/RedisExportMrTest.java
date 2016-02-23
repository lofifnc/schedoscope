package org.schedoscope.export;

import static com.lordofthejars.nosqlunit.redis.EmbeddedRedis.EmbeddedRedisRuleBuilder.newEmbeddedRedisRule;
import static com.lordofthejars.nosqlunit.redis.RedisRule.RedisRuleBuilder.newRedisRule;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.schedoscope.export.outputformat.RedisOutputFormat;
import org.schedoscope.export.utils.RedisMRUtils;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.redis.EmbeddedRedis;
import com.lordofthejars.nosqlunit.redis.RedisRule;

public class RedisExportMrTest extends HiveUnitBaseTest {

	@ClassRule
	public static EmbeddedRedis embeddedRedis = newEmbeddedRedisRule().build();

	@Rule
	public RedisRule redisRule = newRedisRule().defaultEmbeddedRedis();

	// @Inject
	// Jedis jedis;


	@Before
	public void setUp() throws Exception {
		super.setUp();
		setUpHiveServer("src/test/resources/ogm_event_features_data.txt",
				"src/test/resources/ogm_event_features.hql",
				"ogm_event_features");
	}

	@Test
	@UsingDataSet(loadStrategy=LoadStrategyEnum.DELETE_ALL)
	public void testRedisStringExport() throws Exception {

		final String KEY = "visitor_id";
		final String VALUE = "created_at";

		conf.set(RedisExportMapper.REDIS_EXPORT_KEY_NAME, KEY);
		conf.set(RedisExportMapper.REDIS_EXPORT_VALUE_NAME, VALUE);

		Job job = Job.getInstance(conf);

		Class<?> klass  = RedisMRUtils.getRedisWritableKlass(hcatInputSchema, VALUE);

		job.setMapperClass(RedisExportMapper.class);
		job.setReducerClass(RedisExportReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(klass);
		job.setOutputKeyClass(klass);
		job.setOutputValueClass(NullWritable.class);

		assertTrue(job.waitForCompletion(true));
	}

	@Test
	@UsingDataSet(loadStrategy=LoadStrategyEnum.DELETE_ALL)
	public void testRedisMapExport() throws Exception {

		final String KEY = "visitor_id";
		final String VALUE = "uri_path_hashed_count";

		conf.set(RedisExportMapper.REDIS_EXPORT_KEY_NAME, KEY);
		conf.set(RedisExportMapper.REDIS_EXPORT_VALUE_NAME, VALUE);

		Job job = Job.getInstance(conf);

		Class<?> OutputKlass  = RedisMRUtils.getRedisWritableKlass(hcatInputSchema, VALUE);

		job.setMapperClass(RedisExportMapper.class);
		job.setReducerClass(RedisExportReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(HCatInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutputKlass);
		job.setOutputKeyClass(OutputKlass);
		job.setOutputValueClass(NullWritable.class);

		assertTrue(job.waitForCompletion(true));
	}
}
