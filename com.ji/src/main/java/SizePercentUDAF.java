import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;


public class SizePercentUDAF extends UserDefinedAggregateFunction {

     @Override
    public StructType inputSchema() {
         List<StructField> inputFields = new ArrayList<>();
         inputFields.add(DataTypes.createStructField("size", DataTypes.LongType, true));
         inputFields.add(DataTypes.createStructField("create_time", DataTypes.TimestampType, true));
        return DataTypes.createStructType(inputFields);
    }

    @Override
    public StructType bufferSchema() {
        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("firstSize", DataTypes.LongType, true));
        bufferFields.add(DataTypes.createStructField("endSize", DataTypes.LongType, true));
        bufferFields.add(DataTypes.createStructField("min_time", DataTypes.TimestampType, true));
        bufferFields.add(DataTypes.createStructField("max_time", DataTypes.TimestampType, true));
        return DataTypes.createStructType(bufferFields);
    }
    //输出数据类型
    @Override
    public DataType dataType() {
        return DataTypes.StringType;
    }
    //当前函数若输入一样，输出是否一样
    @Override
    public boolean deterministic() {
        return true;
    }
    //初始化buffer中的内容
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0L);
        buffer.update(1, 0L);
        buffer.update(2, null);
        buffer.update(3, null);
    }

    //当传入一条row数据后，更新buffer中的数据
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        //若buffer中min_Time不为空并且输入时间在min_Time之前，更新该数据到buffer中
        if (buffer.getTimestamp(2) != null && input.getTimestamp(1).before(buffer.getTimestamp(2))) {
            buffer.update(0, input.getLong(0));
            buffer.update(2, input.getTimestamp(1));
        }
        //若buffer中max_time不为空并且输入时间在max_time之后，更新该数据到buffer中
        if (buffer.getTimestamp(3) != null && input.getTimestamp(1).after(buffer.getTimestamp(3))) {
            buffer.update(1, input.getLong(0));
            buffer.update(3, input.getTimestamp(1));
        }
        //将第一条数据写入buffer
        if (buffer.getTimestamp(2) == null) {
            buffer.update(0, input.getLong(0));
            buffer.update(1, input.getLong(0));
            buffer.update(2, input.getTimestamp(1));
            buffer.update(3, input.getTimestamp(1));
        }
    }


    //合并两个buffer，并将合并后的数据传给第一个buffer
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        if (buffer1.getTimestamp(2) == null) {
            buffer1.update(0, buffer2.getLong(0));
            buffer1.update(1, buffer2.getLong(1));
            buffer1.update(2, buffer2.getTimestamp(2));
            buffer1.update(3, buffer2.getTimestamp(3));
        }
        if (buffer2.getTimestamp(2).before(buffer1.getTimestamp(2))) {
            buffer1.update(0, buffer2.getLong(0));
            buffer1.update(2, buffer2.getTimestamp(2));
        }
        if (buffer2.getTimestamp(3).after(buffer1.getTimestamp(3))) {
            buffer1.update(1, buffer2.getLong(1));
            buffer1.update(3, buffer2.getTimestamp(3));
        }
    }
    //计算最终产生的结果
    @Override
    public Object evaluate(Row buffer) {
        DecimalFormat df = new DecimalFormat("#.##");
        long firstSize = buffer.getLong(0);
        long minusSize = buffer.getLong(1) - buffer.getLong(0);
        Double value = 0.0D;
        if (minusSize == 0L) {
            value = 0.0D;
        } else if (firstSize != 0L) {
            double d = (double) (minusSize) / (double) firstSize;
            value = d * 100;
        } else {
            value = Double.MAX_VALUE;
        }
        String percentValue = df.format(value);
        value = Double.parseDouble(percentValue);
        return String.valueOf(minusSize) + "," + value;
    }
}
