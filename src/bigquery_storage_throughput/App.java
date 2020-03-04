package bigquery_storage_throughput;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ShardingStrategy;
import com.google.cloud.bigquery.storage.v1beta1.Storage.StreamPosition;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto.TableReference;

/*
 * Not currently used, to enable reading rows add consumer.consume(batch.getAvroRows())
 * to main consumer loop and construct instance from the Avro schema:
 * 
 * 
 * ResultConsumer consumer = new ResultConsumer(new Schema.Parser().parse(session.getAvroSchema().getSchema()));

class ResultConsumer {
	private Schema _schema;
	private DatumReader<GenericRecord> _reader;
	private BinaryDecoder _decoder = null;
	private GenericRecord _row = null;
	
	ResultConsumer(Schema schema) {
		_schema = schema;
		_reader = new GenericDatumReader<>(schema);
	}
	
	void consume(AvroRows rows) throws Exception {
		_decoder = DecoderFactory.get().binaryDecoder(rows.getSerializedBinaryRows().toByteArray(), _decoder);
		Object o = null;
		while (!_decoder.isEnd()) {
			_row = _reader.read(_row, _decoder);
			for (Field field: _schema.getFields()) {
				o = _row.get(field.name());
			}
		}
	}
}
*/

class QueryThread implements Runnable {
	private final TableId _source;
	private final HashMap<String, String> _context;
	
	QueryThread(TableId source) {
		_source = source;
		_context = new HashMap<String, String>();
	}
	
	@Override
	public void run() {
		try {
			_context.put("thread", "" + Thread.currentThread().getId());
			_context.put("src", _source.getProject() + "." + _source.getDataset() + "." + _source.getTable());
			
			BigQueryStorageClient stg = BigQueryStorageClient.create();
			TableReference destinationRef = TableReference
					.newBuilder()
					.setProjectId(_source.getProject())
					.setDatasetId(_source.getDataset())
					.setTableId(_source.getTable())
					.build();
			
			CreateReadSessionRequest sessionRequest = CreateReadSessionRequest
					.newBuilder()
					.setParent("projects/" + _source.getProject())
					.setTableReference(destinationRef)
					// Emulate driver behavior; currently produces a single resultset
					// from one Storage stream
					.setShardingStrategy(ShardingStrategy.LIQUID)
					.setRequestedStreams(1)
					.build();
			
			App.log(_context, "Initializing read session...");
			ReadSession session = stg.createReadSession(sessionRequest);
			
			StreamPosition position = StreamPosition.newBuilder()
					.setStream(session.getStreams(0))
					.build();
			
			ReadRowsRequest readRequest = ReadRowsRequest
					.newBuilder()
					.setReadPosition(position)
					.build();
			
			_context.put("session", session.getStreams(0).getName());
			App.log(_context, "Performing read...");
			ServerStream<ReadRowsResponse> stream = stg.readRowsCallable().call(readRequest);
			
			long start = System.currentTimeMillis();
			long rows = 0;
			for (ReadRowsResponse batch: stream) {
				rows += batch.getRowCount();
				// Currently not consumed, only raw throughput is tested
			}
			long end = System.currentTimeMillis();
			
			App.log(_context, "Finished in %d ms with %d rows", end - start, rows);
		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}

public class App {
	static final int THREAD_COUNT = 1;
	static final String PROJECT_ID = "CHANGE_ME";
	static final String DATASET_ID = "CHANGE_M";
	static final String TABLE_ID = "CHANGE_ME";
	
	public static void log(Map<String, String> context, String format, Object... objects) {
		StringBuilder contextBuilder = new StringBuilder();
		for (Entry<String, String> keyVal: context.entrySet()) {
			contextBuilder.append(" ").append(keyVal.getKey()).append("='").append(keyVal.getValue()).append("'");
		}
		
		System.out.printf("%d |%s | %s\n", System.currentTimeMillis(), contextBuilder.toString(), String.format(format,  objects));
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("PRESS ENTER TO START");
		System.in.read();
		System.out.println("RUNNING...");
		
		Thread[] threads = new Thread[THREAD_COUNT];
		for (int i = 0; i < THREAD_COUNT; i++) {
			TableId destination = TableId.of(PROJECT_ID, DATASET_ID, TABLE_ID);
			Runnable runner = new QueryThread(destination);
			threads[i] = new Thread(runner);
		}
		
		for (Thread thread: threads) {
			thread.start();
		}
		
		for (Thread thread: threads) {
			thread.join();
		}
	}
}
