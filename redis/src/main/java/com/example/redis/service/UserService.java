package com.example.redis.service;

import static java.lang.String.format;

import com.example.redis.model.User;
import com.example.redis.repository.UserRepository;
import com.github.dockerjava.api.exception.NotFoundException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class UserService {

  private final UserRepository userRepository;

  public void addUser(User user) {
    userRepository.save(user);
  }

  public List<User> findAll() {
    return userRepository.findAll();
  }

  public User findById(String id) {
    return userRepository.findById(id)
        .orElseThrow(() -> new NotFoundException(format("User %s not found", id)));
  }

  public List<User> findByName(String name) {
    return userRepository.findByName(name);
  }

  public void deleteById(String id) {
    userRepository.deleteById(id);
  }
}