# TacticSense Project

## Overview
TacticSense is a football application designed to manage various aspects of the sport, including player management, match scheduling, team organization, and statistical analysis. This project is structured with a frontend built using Angular and a backend powered by Spring Boot.

## Project Structure
The project is divided into two main parts: the frontend and the backend.

### Frontend
- **Framework**: Angular
- **Directory**: `frontend`
- **Components**: 
  - Dashboard
  - Matches
  - Players
  - Statistics
  - Teams
  - Shared components
- **Models**: 
  - Player
  - Match
  - Team
  - Stats
- **Services**: 
  - AuthService
  - PlayerService
  - MatchService
  - TeamService
- **Assets**: Includes images and a Bootstrap 4 HTML5 sports website template.

### Backend
- **Framework**: Spring Boot
- **Directory**: `backend`
- **Controllers**: 
  - PlayerController
  - MatchController
  - TeamController
- **Models**: 
  - Player
  - Match
  - Team
- **Repositories**: 
  - PlayerRepository
  - MatchRepository
  - TeamRepository
- **Services**: 
  - PlayerService
  - MatchService
  - TeamService

## Getting Started
To get started with the TacticSense project, follow these steps:

1. **Clone the Repository**: Clone the project repository to your local machine.
2. **Frontend Setup**:
   - Navigate to the `frontend` directory.
   - Install dependencies using `npm install`.
   - Run the application using `ng serve`.
3. **Backend Setup**:
   - Navigate to the `backend` directory.
   - Build the project using Maven with `mvn clean install`.
   - Run the Spring Boot application using `mvn spring-boot:run`.

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.