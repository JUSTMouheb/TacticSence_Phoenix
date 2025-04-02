# TacticSense Backend Documentation

## Overview
TacticSense is a football application designed to manage various aspects of the game, including player management, match scheduling, team organization, and statistics tracking. This backend is built using Spring Boot, providing a robust and scalable solution for handling requests and managing data.

## Project Structure
The backend project is organized as follows:

- **src/main/java/com/tacticsense**: Contains the main application code.
  - **config**: Configuration classes for the Spring Boot application.
  - **controller**: REST controllers for handling HTTP requests related to players, matches, and teams.
  - **model**: Data model classes representing the core entities of the application.
  - **repository**: Interfaces for database operations related to the core entities.
  - **service**: Business logic classes for managing players, matches, and teams.
  - **TacticSenseApplication.java**: The entry point of the Spring Boot application.

- **src/main/resources**: Contains configuration files and static resources.
  - **application.properties**: Configuration properties for the Spring Boot application.
  - **static**: Directory for static resources served by the application.
  - **templates**: Directory for Thymeleaf templates used in the application.

- **src/test/java/com/tacticsense**: Contains test classes for the backend application.

## Getting Started
To run the backend application, ensure you have Java and Maven installed. Follow these steps:

1. Clone the repository or download the project files.
2. Navigate to the `backend` directory.
3. Run the following command to build the project:
   ```
   mvn clean install
   ```
4. Start the application using:
   ```
   mvn spring-boot:run
   ```

## API Endpoints
The backend exposes several RESTful API endpoints for interacting with the application. Refer to the respective controller classes for detailed information on available endpoints and their usage.

## Dependencies
The project uses Maven for dependency management. Check the `pom.xml` file for a list of dependencies required for the backend application.

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.