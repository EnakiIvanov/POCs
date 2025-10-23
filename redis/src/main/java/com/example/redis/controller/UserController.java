package com.example.redis.controller;

import com.example.redis.model.User;
import com.example.redis.service.UserService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/users")
public class UserController {

  private final UserService userService;

  @GetMapping("/{id}")
  public User findById(@PathVariable String id) {
    return userService.findById(id);
  }

  @GetMapping("/")
  public List<User> findByName(@RequestParam String name) {
    return userService.findByName(name);
  }

  @GetMapping
  public List<User> findAll() {
    return userService.findAll();
  }

  @PostMapping
  public void addUser(@RequestBody User user) {
    userService.addUser(user);
  }

  @DeleteMapping("/{id}")
  public void removeUserById(@PathVariable String id) {
    userService.deleteById(id);
  }
}