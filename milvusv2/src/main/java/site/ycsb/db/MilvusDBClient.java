package site.ycsb.db;


import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;


import com.google.common.collect.Lists;
import io.milvus.client.MilvusClient;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.param.*;

import java.util.*;

import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.highlevel.collection.CreateSimpleCollectionParam;
import io.milvus.param.*;
import io.milvus.param.highlevel.dml.DeleteIdsParam;
import io.milvus.param.highlevel.dml.GetIdsParam;
import io.milvus.param.highlevel.dml.QuerySimpleParam;
import io.milvus.param.dml.UpsertParam;
import io.milvus.param.highlevel.dml.response.DeleteResponse;
import io.milvus.param.highlevel.dml.response.GetResponse;
import io.milvus.param.highlevel.dml.response.QueryResponse;
import io.milvus.response.DescCollResponseWrapper;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.FieldDataWrapper;
import io.milvus.grpc.QueryResults;
import site.ycsb.*;

import static io.milvus.grpc.MsgType.Upsert;

public class MilvusDBClient extends DB {

  public MilvusClient client;

  @Override
  public void init() throws DBException {
    ConnectParam connectParam = ConnectParam.newBuilder()
        .withHost("localhost")
        .withPort(19530)
        .withAuthorization("root", "Milvus")
        .build();
    client = new MilvusServiceClient(connectParam);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    List<String> ids = Lists.newArrayList(key);
    GetIdsParam getParam = GetIdsParam.newBuilder()
        .withCollectionName(table)
        .withPrimaryIds(ids)
        .withOutputFields(Lists.newArrayList("*"))
        .build();

    R<GetResponse> response5 = client.get(getParam);
    if (response5.getStatus() != R.Status.Success.getCode()) {
      System.out.println(response5.getMessage());
      return Status.ERROR;
    }
    for (QueryResultsWrapper.RowRecord rowRecord : response5.getData().getRowRecords()) {
      for (String field : fields) {
        String value = rowRecord.get(field).toString();
        result.put(field, new StringByteIterator(value));
      }
    }

    return Status.OK;
  }
  @Override

  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    StringBuilder filter = new StringBuilder("id >= ");
    filter.append(startkey);
    QuerySimpleParam querySimpleParam = QuerySimpleParam.newBuilder()
        .withCollectionName(table)
        .withOutputFields(Lists.newArrayList("*"))
        .withFilter(filter.toString())
        .withLimit((long) recordcount)
        .withOffset(0L)
        .build();
    R<QueryResponse> response6 = client.query(querySimpleParam);
    if (response6.getStatus() != R.Status.Success.getCode()) {
      System.out.println(response6.getMessage());
      return Status.ERROR;
    }
    for (QueryResultsWrapper.RowRecord rowRecord : response6.getData().getRowRecords()) {
      HashMap<String, ByteIterator> row = new HashMap<>();
      for (String field : fields) {
        String value = rowRecord.get(field).toString();
        row.put(field, new StringByteIterator(value));
      }
      result.add(row);
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    List<Long> ids = new ArrayList<>();
    ids.add(Long.parseLong(key));
    List<List<Float>> vectors = new ArrayList<>();
    List<Float> vector = new ArrayList<>();
    vector.add(0.0f);
    vector.add(0.0f);
    vectors.add(vector);
    List<UpsertParam.Field> fieldsInsert = new ArrayList<>();
    fieldsInsert.add(new UpsertParam.Field("YCSB_KEY", ids));
    fieldsInsert.add(new UpsertParam.Field("vector", vectors));
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldsInsert.add(new UpsertParam.Field(entry.getKey(), Lists.newArrayList(entry.getValue().toString())));
    }
    UpsertParam param4 = UpsertParam.newBuilder()
        .withCollectionName(table)
        .withFields(fieldsInsert)
        .build();
    R<MutationResult> response4 = client.upsert(param4);
    if (response4.getStatus() != R.Status.Success.getCode()) {
      System.out.println(response4.getMessage());
      return Status.ERROR;
    } else {
      return Status.OK;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    List<Long> ids = new ArrayList<>();
    ids.add(Long.parseLong(key));
    List<List<Float>> vectors = new ArrayList<>();
    List<Float> vector = new ArrayList<>();
    vector.add(0.0f);
    vector.add(0.0f);
    vectors.add(vector);
    List<InsertParam.Field> fieldsInsert = new ArrayList<>();
    fieldsInsert.add(new InsertParam.Field("YCSB_KEY", ids));
    fieldsInsert.add(new InsertParam.Field("vector", vectors));
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldsInsert.add(new InsertParam.Field(entry.getKey(), Lists.newArrayList(entry.getValue().toString())));
    }
    InsertParam param4 = InsertParam.newBuilder()
        .withCollectionName(table)
        .withFields(fieldsInsert)
        .build();
    R<MutationResult> response4 = client.insert(param4);
    if (response4.getStatus() != R.Status.Success.getCode()) {
      return Status.ERROR;
    } else {
      return Status.OK;
    }
  }

  @Override
  public Status delete(String table, String key) {
    List<String> ids = Lists.newArrayList(key);
    DeleteIdsParam param8 = DeleteIdsParam.newBuilder()
        .withCollectionName(table)
        .withPrimaryIds(ids)
        .build();

    R<DeleteResponse> response8 = client.delete(param8);
    if (response8.getStatus() != R.Status.Success.getCode()) {
      System.out.println(response8.getMessage());
      return Status.ERROR;
    }

    return Status.OK;
  }
}
