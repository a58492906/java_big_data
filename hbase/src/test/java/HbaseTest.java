import hadoop.HbaseService;
import org.junit.Test;

import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @author xjm
 * @version 1.0
 * @date 2022-05-12 20:23
 */
public class HbaseTest {
    @Test
    public void create() {
        try (Connection connection = HbaseService.createConnection();) {
            HbaseService.createNamespace(connection,"xuejm");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createTable() {
        try (Connection connection = HbaseService.createConnection();) {
            String[] strings = new String[]{"name", "score", "info"};
            HbaseService.createTable(connection,"xuejm:student",strings);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void scan() {
        try (Connection connection = HbaseService.createConnection()){
            HbaseService.scan(connection,"xuejm:student");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void insert() {
        try (Connection connection = HbaseService.createConnection()) {
            HbaseService.insert(connection, "xuejm:student", "row1", "name", "", "xue");
            HbaseService.insert(connection, "xuejm:student", "row1", "info", "student_id", "1");
            HbaseService.insert(connection, "xuejm:student", "row1", "info", "class", "1");
            HbaseService.insert(connection, "xuejm:student", "row1", "score", "math", "75");
            HbaseService.insert(connection, "xuejm:student", "row2", "name", "", "wang");
            HbaseService.insert(connection, "xuejm:student", "row2", "info", "student_id", "2");
            HbaseService.insert(connection, "xuejm:student", "row2", "info", "class", "2");
            HbaseService.insert(connection, "xuejm:student", "row2", "score", "math", "80");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void deleteRow() {
        try (Connection connection = HbaseService.createConnection()){
            HbaseService.deleteRow(connection, "xuejm:student", "row1");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
