package com.example.redis.repository;

import com.example.redis.model.User;
import java.util.List;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends ListCrudRepository<User, String> {

  List<User> findByName(String name);
}