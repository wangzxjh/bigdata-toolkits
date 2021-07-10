package org.huawei.jdbc;

import com.google.common.base.Preconditions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

public abstract class JdbcClient implements AutoCloseable {

    public static final String USER = "user";
    public static final String PASSWORD = "password";
    protected final String url;
    protected final Properties properties = new Properties();
    protected Connection connection;
    private boolean intialed = false;

    public JdbcClient(String url, String user, String passwd) {
        this.url = url;
        Optional.ofNullable(user).ifPresent(u -> properties.setProperty(USER, u));
        Optional.ofNullable(passwd).ifPresent(p -> properties.setProperty(PASSWORD, p));
    }

    public JdbcClient(String url, String user, String passwd, Properties... properties) {
        this.url = url;
        Optional.ofNullable(user).ifPresent(u -> this.properties.setProperty(USER, u));
        Optional.ofNullable(passwd).ifPresent(p -> this.properties.setProperty(PASSWORD, p));
        Arrays.stream(properties).forEach(p -> p.forEach(this.properties::put));
    }

    public JdbcClient(String url, Properties properties) {
        this.url = url;
        properties.forEach(this.properties::put);
    }


    protected abstract void loadDriverClass() throws ClassNotFoundException;

    public void init() throws SQLException, ClassNotFoundException {
        if (intialed) {
            return;
        }
        intialed = true;

        // 1. load driver class
        loadDriverClass();
        //2.get database connection
        connection = DriverManager.getConnection(url, properties);
    }

    public void executeQuery(String sql, QueryCallBack queryCallBack) throws SQLException {
        Preconditions.checkState(intialed, this.getClass().getSimpleName() + " has not be initialed");

        try (Statement statement = connection.prepareStatement(sql)) {
            queryCallBack.call(statement.executeQuery(sql));
        }
    }

    public void executeUpdate(String sql, UpdateCallBack callBack) throws SQLException {
        Preconditions.checkState(intialed, this.getClass().getSimpleName() + " has not be initialed");

        try (Statement statement = connection.prepareStatement(sql)) {
            callBack.call(statement.executeUpdate(sql));
        }
    }

    public void execute(String sql, ExecuteCallBack callBack) throws SQLException {
        Preconditions.checkState(intialed, this.getClass().getSimpleName() + " has not be initialed");

        try (Statement statement = connection.prepareStatement(sql)) {
            callBack.call(statement.execute(sql));
        }
    }

    public void execute(String sql) throws SQLException {
        execute(sql, success -> {
        });
    }

    public void executeUpdate(String sql) throws SQLException {
        executeUpdate(sql, number -> {
        });
    }




    @Override
    public void close() throws SQLException {
        Preconditions.checkState(intialed, this.getClass().getSimpleName() + " has not be initialed");

        connection.close();
    }

    public static interface QueryCallBack {
        void call(ResultSet resultSet) throws SQLException;
    }

    public static interface UpdateCallBack {
        void call(int number);
    }

    public static interface ExecuteCallBack {
        void call(boolean success);
    }

}
