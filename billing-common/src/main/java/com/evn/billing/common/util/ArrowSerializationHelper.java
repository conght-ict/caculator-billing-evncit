package com.evn.billing.common.util;

import com.evn.billing.common.dto.MeterReadingDto;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ArrowSerializationHelper {

    private static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    private static Schema createSchema() {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("meterPointId", FieldType.nullable(new ArrowType.Utf8()), null));
        fields.add(new Field("fromDate", FieldType.nullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field("toDate", FieldType.nullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field("startIndex", FieldType.nullable(new ArrowType.Decimal(15, 4, 128)), null));
        fields.add(new Field("endIndex", FieldType.nullable(new ArrowType.Decimal(15, 4, 128)), null));
        fields.add(new Field("consumption", FieldType.nullable(new ArrowType.Decimal(15, 4, 128)), null));
        return new Schema(fields);
    }

    /**
     * Serializes a list of MeterReadingDto into Apache Arrow binary format.
     */
    public static byte[] serializeReadings(List<MeterReadingDto> readings) {
        if (readings == null || readings.isEmpty()) {
            return new byte[0];
        }

        Schema schema = createSchema();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             VarCharVector meterPointIdVector = (VarCharVector) root.getVector("meterPointId");
             BigIntVector fromDateVector = (BigIntVector) root.getVector("fromDate");
             BigIntVector toDateVector = (BigIntVector) root.getVector("toDate");
             DecimalVector startIndexVector = (DecimalVector) root.getVector("startIndex");
             DecimalVector endIndexVector = (DecimalVector) root.getVector("endIndex");
             DecimalVector consumptionVector = (DecimalVector) root.getVector("consumption");
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {

            int size = readings.size();
            root.allocateNew();

            for (int i = 0; i < size; i++) {
                MeterReadingDto dto = readings.get(i);
                
                // Write meterPointId
                if (dto.getMaDdo() != null) {
                    meterPointIdVector.setSafe(i, dto.getMaDdo().getBytes());
                }
                
                // Write dates as epoch milliseconds
                if (dto.getTuNgay() != null) {
                    fromDateVector.setSafe(i, dto.getTuNgay().toInstant(ZoneOffset.UTC).toEpochMilli());
                }
                if (dto.getDenNgay() != null) {
                    toDateVector.setSafe(i, dto.getDenNgay().toInstant(ZoneOffset.UTC).toEpochMilli());
                }
                
                // Write Decimals
                if (dto.getChiSoDau() != null) {
                    startIndexVector.setSafe(i, dto.getChiSoDau());
                }
                if (dto.getChiSoCuoi() != null) {
                    endIndexVector.setSafe(i, dto.getChiSoCuoi());
                }
                if (dto.getSanLuong() != null) {
                    consumptionVector.setSafe(i, dto.getSanLuong());
                }
            }

            root.setRowCount(size);
            writer.start();
            writer.writeBatch();
            writer.end();
            
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize readings to Apache Arrow format", e);
        }
    }

    /**
     * Deserializes Apache Arrow binary bytes back into a list of MeterReadingDto.
     */
    public static List<MeterReadingDto> deserializeReadings(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return Collections.emptyList();
        }

        List<MeterReadingDto> list = new ArrayList<>();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);

        try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
            reader.loadNextBatch();
            try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                VarCharVector meterPointIdVector = (VarCharVector) root.getVector("meterPointId");
                BigIntVector fromDateVector = (BigIntVector) root.getVector("fromDate");
                BigIntVector toDateVector = (BigIntVector) root.getVector("toDate");
                DecimalVector startIndexVector = (DecimalVector) root.getVector("startIndex");
                DecimalVector endIndexVector = (DecimalVector) root.getVector("endIndex");
                DecimalVector consumptionVector = (DecimalVector) root.getVector("consumption");

                int rowCount = root.getRowCount();
                for (int i = 0; i < rowCount; i++) {
                    MeterReadingDto dto = new MeterReadingDto();
                    
                    if (!meterPointIdVector.isNull(i)) {
                        dto.setMaDdo(meterPointIdVector.getObject(i).toString());
                    }
                    if (!fromDateVector.isNull(i)) {
                        dto.setTuNgay(LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(fromDateVector.get(i)), ZoneOffset.UTC));
                    }
                    if (!toDateVector.isNull(i)) {
                        dto.setDenNgay(LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(toDateVector.get(i)), ZoneOffset.UTC));
                    }
                    if (!startIndexVector.isNull(i)) {
                        dto.setChiSoDau(startIndexVector.getObject(i));
                    }
                    if (!endIndexVector.isNull(i)) {
                        dto.setChiSoCuoi(endIndexVector.getObject(i));
                    }
                    if (!consumptionVector.isNull(i)) {
                        dto.setSanLuong(consumptionVector.getObject(i));
                    }
                    list.add(dto);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize readings from Apache Arrow format", e);
        }
        return list;
    }
}
