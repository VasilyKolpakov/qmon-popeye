package popeye.analytics.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import popeye.analytics.OtsdbHfileReader;
import popeye.storage.hbase.BytesKey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class JavaOtsdbAnalytics {

  public static final String NAMES_COLUMN_DIR_KEY = "popeye.analytics.hadoop.JavaOtsdbAnalytics.namesDir";
  private final static int METRIC_LENGTH = 3;
  private final static int TAG_KEY_LENGTH = 3;
  private final static int TAG_VALUE_LENGTH = 3;
  private final static int TAG_LENGTH = TAG_KEY_LENGTH + TAG_VALUE_LENGTH;
  public static final String NUMBER_OF_POINTS_NAMED_OUTPUT = "numberOfPoints";
  public static final String TAGS_NAMED_OUTPUT = "tags";

  public static void prepareJob(Job job, String nameColumnDirPath) {
    MultipleOutputs.addNamedOutput(
      job,
      JavaOtsdbAnalytics.NUMBER_OF_POINTS_NAMED_OUTPUT,
      OrcNewOutputFormat.class,
      NullWritable.class,
      Writable.class
    );
    MultipleOutputs.addNamedOutput(
      job,
      JavaOtsdbAnalytics.TAGS_NAMED_OUTPUT,
      OrcNewOutputFormat.class,
      NullWritable.class,
      Writable.class
    );
    job.getConfiguration().set(NAMES_COLUMN_DIR_KEY, nameColumnDirPath, "job config");
  }

  public static class OAMapper extends Mapper<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, BaseTimeAndDeltasWritable> {
    /**
     * Number of LSBs in time_deltas reserved for flags.
     */
    private final static short FLAG_BITS = 4;
    private final static int tagsOffset = METRIC_LENGTH + 4;
    private Text word = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, KeyValue value, Context context) throws IOException, InterruptedException {
      byte[] row = key.get();
      byte[] rowWithoutBaseTime = new byte[row.length - 4];
      // copy metric
      System.arraycopy(row, 0, rowWithoutBaseTime, 0, METRIC_LENGTH);
      // copy tags
      System.arraycopy(row, tagsOffset, rowWithoutBaseTime, METRIC_LENGTH, row.length - tagsOffset);
      int baseTime = Bytes.toInt(row, METRIC_LENGTH);
      short[] timestamps = getDeltas(value);
      context.write(
        new ImmutableBytesWritable(rowWithoutBaseTime),
        new BaseTimeAndDeltasWritable(baseTime, timestamps)
      );
    }

    private short[] getDeltas(KeyValue keyValue) {
      int qLength = keyValue.getQualifierLength();
      assert (qLength % 2 == 0);
      int numberOfPointsInKv = qLength / 2;
      byte[] qualArray = keyValue.getQualifierArray();
      int qualOffset = keyValue.getQualifierOffset();
      short[] deltas = new short[numberOfPointsInKv];
      for (int i = 0; i < numberOfPointsInKv; i++) {
        short qualShort = Bytes.toShort(qualArray, qualOffset + i * 2);
        short delta = (short) ((qualShort & 0xFFFF) >>> FLAG_BITS);
        deltas[i] = delta;
      }
      return deltas;
    }
  }

  public static class OAReducer extends Reducer<ImmutableBytesWritable, BaseTimeAndDeltasWritable, NullWritable, Writable> {
    private final OrcSerde serde = new OrcSerde();
    private ObjectInspector inspector;
    private final String pointsCountTypeString = "struct<metric:string,tags:string,base_time:int,points_count:int>";
    private final String tagsTypeString = "struct<metric:string,tags:string,tag_key:string,tag_value:string>";

    private final ObjectInspector pointsCountOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
      TypeInfoUtils.getTypeInfoFromTypeString(pointsCountTypeString)
    );

    private final ObjectInspector tagsOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
      TypeInfoUtils.getTypeInfoFromTypeString(tagsTypeString)
    );

    private OtsdbHfileReader.IdMappingJava names;
    private MultipleOutputs<NullWritable, Writable> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      Configuration configuration = context.getConfiguration();
      FileSystem fs = FileSystem.get(configuration);
      CacheConfig cc = new CacheConfig(configuration);
      Path path = new Path(configuration.get(NAMES_COLUMN_DIR_KEY));
      names = OtsdbHfileReader.loadAllNamesJava(fs, cc, path);
      mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<BaseTimeAndDeltasWritable> values, Context context) throws IOException, InterruptedException {
      ParsedRow parsedRow = parseRow(key.copyBytes());
      String tagsString = createTagsString(parsedRow);
      HashSet<Short> deltasSet = new HashSet<>();
      for (BaseTimeAndDeltasWritable val : values) {
        deltasSet.clear();
        short[] deltas = val.getDeltas();
        for (int i = 0; i < deltas.length; i++) {
          deltasSet.add(deltas[i]);
        }
        List<Object> pointsCountStruct =
          Arrays.<Object>asList(parsedRow.metric, tagsString, val.getBaseTime(), deltasSet.size());
        mos.write(NUMBER_OF_POINTS_NAMED_OUTPUT, NullWritable.get(), serde.serialize(pointsCountStruct, pointsCountOI));
      }
      for (List<String> struct : createTagsStructs(parsedRow, tagsString)) {
        mos.write(TAGS_NAMED_OUTPUT, NullWritable.get(), serde.serialize(struct, tagsOI));
      }
    }

    private List<List<String>> createTagsStructs(ParsedRow parsedRow, String tagsString) {
      int numberOfTags = parsedRow.tagKeys.size();
      List<List<String>> rows = new ArrayList<>(numberOfTags);
      for (int i = 0; i < numberOfTags; i++) {
        String tagKey = parsedRow.tagKeys.get(i);
        String tagValue = parsedRow.tagValues.get(i);
        rows.add(Arrays.asList(parsedRow.metric, tagsString, tagKey, tagValue));
      }
      return rows;
    }

    private String createTagsString(ParsedRow parsedRow) {
      StringBuilder tagsStringBuf = new StringBuilder();
      int numberOfTags = parsedRow.tagKeys.size();
      for (int i = 0; i < numberOfTags; i++) {
        String tagKey = parsedRow.tagKeys.get(i);
        String tagValue = parsedRow.tagValues.get(i);
        tagsStringBuf.append(tagKey).append('=').append(tagValue);
        if (i != numberOfTags - 1) {
          tagsStringBuf.append(',');
        }
      }
      return tagsStringBuf.toString();
    }

    private ParsedRow parseRow(byte[] rowWithoutBaseTime) {
      BytesKey metricId = new BytesKey(Arrays.copyOfRange(rowWithoutBaseTime, 0, METRIC_LENGTH));
      String metric = names.metrics().get(metricId);
      int numberOfTags = (rowWithoutBaseTime.length - METRIC_LENGTH) / TAG_LENGTH;
      List<String> tagKeys = new ArrayList<>(numberOfTags);
      List<String> tagValues = new ArrayList<>(numberOfTags);
      for (int i = 0; i < numberOfTags; i++) {
        int tagKeyIdOffset = METRIC_LENGTH + i * 6;
        BytesKey tagKeyId = new BytesKey(Arrays.copyOfRange(rowWithoutBaseTime, tagKeyIdOffset, tagKeyIdOffset + 3));
        BytesKey tagValueId = new BytesKey(Arrays.copyOfRange(rowWithoutBaseTime, tagKeyIdOffset + 3, tagKeyIdOffset + 6));

        String tagKey = names.tagKeys().get(tagKeyId);
        String tagValue = names.tagValues().get(tagValueId);

        tagKeys.add(tagKey);
        tagValues.add(tagValue);
      }
      return new ParsedRow(metric, tagKeys, tagValues);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      mos.close();
    }
  }

  public static class BaseTimeAndDeltasWritable implements Writable {
    private int baseTime;
    private short[] deltas;

    // default constructor is required
    public BaseTimeAndDeltasWritable() {
    }

    public BaseTimeAndDeltasWritable(int baseTime, short[] deltas) {
      this.baseTime = baseTime;
      this.deltas = deltas;
    }

    public int getBaseTime() {
      return baseTime;
    }

    public short[] getDeltas() {
      return deltas;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(baseTime);
      out.writeInt(deltas.length);
      for (int i = 0; i < deltas.length; i++) {
        out.writeShort(deltas[i]);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      baseTime = in.readInt();
      int size = in.readInt();
      deltas = new short[size];
      for (int i = 0; i < deltas.length; i++) {
        deltas[i] = in.readShort();
      }
    }
  }

  public static class ParsedRow {
    public final String metric;
    public final List<String> tagKeys;
    public final List<String> tagValues;

    public ParsedRow(String metric, List<String> tagKeys, List<String> tagValues) {
      assert tagKeys.size() == tagValues.size();
      this.metric = metric;
      this.tagKeys = tagKeys;
      this.tagValues = tagValues;
    }
  }

}
