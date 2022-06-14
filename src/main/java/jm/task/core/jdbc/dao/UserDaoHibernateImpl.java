package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import javax.persistence.PersistenceException;
import java.util.ArrayList;
import java.util.List;


public class UserDaoHibernateImpl implements UserDao {
    private static final SessionFactory SESSION_FACTORY = Util.getSessionFactory();
    private String sql;

    public UserDaoHibernateImpl() {

    }


    @Override
    public void createUsersTable() {
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction transaction = session.beginTransaction();
            sql = "CREATE TABLE IF NOT EXISTS Users " +
                    "(ID BIGINT PRIMARY KEY AUTO_INCREMENT, Name VARCHAR(255), LastName VARCHAR(255), Age TINYINT)";
            session.createSQLQuery(sql).executeUpdate();
            transaction.commit();
            session.clear();
            session.close();
        } catch (PersistenceException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void dropUsersTable() {
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction transaction = session.beginTransaction();
            sql = "DROP TABLE IF EXISTS Users";
            session.createSQLQuery(sql).executeUpdate();
            transaction.commit();
            session.clear();
            session.close();
        } catch (PersistenceException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction transaction = session.beginTransaction();
            session.persist(new User(name, lastName, age));
            transaction.commit();
            session.clear();
            session.close();
        } catch (PersistenceException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeUserById(long id) {
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction transaction = session.beginTransaction();
            sql = "DELETE FROM Users WHERE ID = ?";
            session.createSQLQuery(sql).setParameter(1, id).executeUpdate();
            transaction.commit();
            session.clear();
            session.close();
        } catch (PersistenceException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<User> getAllUsers() {
        try (Session session = SESSION_FACTORY.openSession()) {
            return session.createQuery("from User", User.class).list();
        } catch (HibernateException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public void cleanUsersTable() {
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction transaction = session.beginTransaction();
            sql = "TRUNCATE TABLE Users";
            session.createSQLQuery(sql).executeUpdate();
            transaction.commit();
            session.clear();
            session.close();
        } catch (PersistenceException e) {
            e.printStackTrace();
        }
    }
}