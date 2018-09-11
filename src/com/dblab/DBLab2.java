package com.dblab;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Javabean class modeling a row in the actor table.
 *
 * @author me
 * @version 1.0
 * @since 2018-07-17
 */
class Actor implements Serializable {
    private int actor_id;
    private String first_name;
    private String last_name;
    private Date last_update;

    /**
     * Default constructor.
     */
    public Actor() {
    }

    /**
     * Getter for actor_id field.
     *
     * @return int actor_id.
     */
    public int getActorId() {
        return actor_id;
    }

    /**
     * Setter for actor_id field.
     *
     * @param actor_id The new actor_id int value to set.
     */
    public void setActorId(int actor_id) {
        this.actor_id = actor_id;
    }

    /**
     * Getter for first_name field.
     *
     * @return String first_name.
     */
    public String getFirstName() {
        return first_name;
    }

    /**
     * Setter for first_name field.
     *
     * @param first_name The new first_name String value to set.
     */
    public void setFirstName(String first_name) {
        this.first_name = first_name;
    }

    /**
     * Getter for last_name field.
     *
     * @return String last_name.
     */
    public String getLastName() {
        return last_name;
    }

    /**
     * Setter for last_name field.
     *
     * @param last_name The new last_name String value to set.
     */
    public void setLastName(String last_name) {
        this.last_name = last_name;
    }

    /**
     * Getter for last_update field.
     *
     * @return java.sql.Date last_update.
     */
    public Date getLastUpdate() {
        return last_update;
    }

    /**
     * Setter for last_update field.
     *
     * @param last_update The new last_update Date value to set.
     */
    public void setLastUpdate(Date last_update) {
        this.last_update = last_update;
    }

    /**
     * Overridden toString method for displaying an actor in string format.
     *
     * @return String concatenation of actor_id, first_name and last_name, separated by pipe characters.
     */
    public String toString() {
        return this.actor_id + " | " + this.first_name + " | " + this.last_name;
    }
}

/**
 * Helper class to manage connection to database and CRUD operations using Actor javabean.
 *
 * @author me
 * @version 1.0
 * @since 2018-07-17
 */
class ActorMgr {

    /**
     * Gets a connection to the database.
     *
     * @return SQLConnection
     * @throws SQLException on connection failure.
     */
    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost/sakila", "me", "itsme123123");
    }

    /**
     * Constructs an Actor instance from the current position of a ResultSet object.
     *
     * @param resultSet The ResultSet currently pointing to an actor row.
     * @return Actor instance.
     */
    private static Actor getActorFromResultSetRow(ResultSet resultSet) {
        Actor actor = new Actor();
        try {
            actor.setActorId(resultSet.getInt("actor_id"));
            actor.setFirstName(resultSet.getString("first_name"));
            actor.setLastName(resultSet.getString("last_name"));
            actor.setLastUpdate(resultSet.getDate("last_update"));
        } catch (SQLException e) {
            System.err.println("SQLException in getActorFromResultSetRow():");
            e.printStackTrace();
        }
        return actor;
    }

    /**
     * Gets all actors in the actor table.
     *
     * @return List of Actor instances.
     */
    public static List<Actor> getAllRecords() {
        List<Actor> actors = new ArrayList<>();
        try (Connection connection = getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery("Select * from actor")) {
                    while (resultSet.next()) {
                        actors.add(getActorFromResultSetRow(resultSet));
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("SQLException in getAllRecords():");
            e.printStackTrace();
        }
        return actors;
    }

    /**
     * Gets a single Actor instance from the actor table.
     *
     * @param actor_id The actor_id of the actor row to retrieve.
     * @return Actor instance.
     */
    public static Actor getRecord(int actor_id) {
        Actor actor = null;
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("Select * from actor where actor_id = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                statement.setInt(1, actor_id);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        actor = getActorFromResultSetRow(resultSet);
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("SQLException in getRecord():");
            e.printStackTrace();
        }
        return actor;
    }

    /**
     * Inserts a new row into the actor table.
     *
     * @param actor The Actor instance to insert.
     * @return boolean representing insertion success.
     */
    public static boolean insertRecord(Actor actor) {
        boolean success = false;
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("Insert into actor (first_name, last_name) values (?, ?)", Statement.RETURN_GENERATED_KEYS)) {
                statement.setString(1, actor.getFirstName());
                statement.setString(2, actor.getLastName());
                success = statement.executeUpdate() > 0;
                if (success) {
                    try (ResultSet keys = statement.getGeneratedKeys()) {
                        if (keys.next()) {
                            System.out.println("Success! Created a new row with ID " + keys.getLong(1) + ".");
                        }
                    }
                } else {
                    System.out.println("Record was not created.");
                }
            }
        } catch (SQLException e) {
            System.err.println("SQLException in insertRecord():");
            e.printStackTrace();
        }
        return success;
    }

    /**
     * Updates an existing row in the actor table.
     *
     * @param actor The Actor instance to update.
     * @return boolean representing update success.
     */
    public static boolean updateRecord(Actor actor) {
        boolean success = false;
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("Update actor set first_name = ?, last_name = ? where actor_id = ?", Statement.RETURN_GENERATED_KEYS)) {
                statement.setString(1, actor.getFirstName());
                statement.setString(2, actor.getLastName());
                statement.setInt(3, actor.getActorId());
                success = statement.executeUpdate() > 0;
                System.out.println(success ? "Success! One record was updated." : "Zero records updated.");
            }
        } catch (SQLException e) {
            System.err.println("SQLException in insertRecord():");
            e.printStackTrace();
        }
        return success;
    }

    /**
     * Deletes a row from the actor table.
     *
     * @param actor_id The actor_id of the actor row to delete.
     * @return boolean representing deletion success.
     */
    public static boolean deleteRecord(int actor_id) {
        boolean success = false;
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("Delete from actor where actor_id = ?")) {
                statement.setInt(1, actor_id);
                success = statement.executeUpdate() > 0;
                System.out.println(success ? "Success! One record was deleted." : "Zero records deleted.");
            }
        } catch (SQLException e) {
            System.err.println("SQLException in insertRecord():");
            e.printStackTrace();
        }
        return success;
    }
}

/**
 * Main class for DB Lab #2
 *
 * @author me
 * @version 1.0
 * @since 2018-07-17
 */
class DBLab2 {

    /**
     * Main method.  Ask the user to choose a command, and handle appropriately using the ActorMgr static methods.
     *
     * @param args Unused.
     */
    public static void main(String[] args) {
        String subCommand;
        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.println("Actor Table");
            System.out.println("1 - Display all rows | 2 - Get a row | 3 - Add a new row | 4 - Update an existing row | 5 - Delete a row");
            System.out.print("Select an option: >");
            String command = input.next();
            switch (command.trim()) {
                case "1":
                    for (Actor a : ActorMgr.getAllRecords()) {
                        System.out.println(a);
                    }
                    break;
                case "2":
                    System.out.println();
                    System.out.print("Enter the actor ID: >");
                    subCommand = input.next();
                    try {
                        int actor_id = Integer.parseInt(subCommand);
                        Actor actor = ActorMgr.getRecord(actor_id);
                        System.out.println(actor != null ? actor : "Actor ID not found");
                    } catch (Exception e) {
                        System.out.println("Invalid actor ID");
                    }
                    break;
                case "3":
                    Actor newActor = new Actor();
                    System.out.println();
                    System.out.print("Enter the first name: >");
                    newActor.setFirstName(input.next());
                    System.out.print("Enter the last name: >");
                    newActor.setLastName(input.next());
                    ActorMgr.insertRecord(newActor);
                    break;
                case "4":
                    List<Actor> actors = ActorMgr.getAllRecords();
                    for (Actor a : actors) {
                        System.out.println(a);
                    }
                    System.out.println();
                    System.out.print("Enter the actor ID: >");
                    subCommand = input.next();
                    try {
                        int actor_id = Integer.parseInt(subCommand);
                        Actor actorToUpdate = ActorMgr.getRecord(actor_id);
                        if (actorToUpdate == null) {
                            throw new Exception("No actor found with ID = " + actor_id);
                        }
                        System.out.println();
                        System.out.print("Enter the first name: >");
                        actorToUpdate.setFirstName(input.next());
                        System.out.print("Enter the last name: >");
                        actorToUpdate.setLastName(input.next());
                        ActorMgr.updateRecord(actorToUpdate);
                        System.out.println();
                        for (Actor a : ActorMgr.getAllRecords()) {
                            System.out.println(a);
                        }
                    } catch (Exception e) {
                        System.out.println("Invalid actor ID");
                    }
                    break;
                case "5":
                    System.out.println();
                    System.out.print("Enter the actor ID: >");
                    subCommand = input.next();
                    try {
                        int actor_id = Integer.parseInt(subCommand);
                        ActorMgr.deleteRecord(actor_id);
                        System.out.println();
                        for (Actor a : ActorMgr.getAllRecords()) {
                            System.out.println(a);
                        }
                    } catch (Exception e) {
                        System.out.println("Invalid actor ID");
                    }
                    break;
                default:
                    System.out.println("Invalid Option");
                    System.exit(0);
            }
            System.out.println();
        }
    }
}
