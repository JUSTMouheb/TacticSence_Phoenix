# TacticSense Frontend

This is the frontend part of the TacticSense application, built using Angular. The application is designed to manage football-related data, including players, matches, teams, and statistics.

## Project Structure

- **src/**: Contains the source code for the Angular application.
  - **app/**: The main application module.
    - **components/**: Contains various components for different features.
      - **dashboard/**: Dashboard related components.
      - **matches/**: Components for managing matches.
      - **players/**: Components for managing players.
      - **statistics/**: Components for displaying statistics.
      - **teams/**: Components for managing teams.
      - **shared/**: Shared components used across the application.
    - **models/**: Contains TypeScript models for the application.
    - **services/**: Contains services for handling business logic and API calls.
  - **assets/**: Contains static assets such as images and the Bootstrap template.
  - **environments/**: Contains environment-specific configurations.
  - **index.html**: The main HTML file for the application.
  - **styles.scss**: Global styles for the application.

## Getting Started

1. **Install Dependencies**: Run `npm install` to install the required dependencies.
2. **Run the Application**: Use `ng serve` to start the development server. Navigate to `http://localhost:4200/` to view the application.

## Build

To build the application for production, run `ng build`. The output will be stored in the `dist/` directory.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any suggestions or improvements.

## License

This project is licensed under the MIT License.