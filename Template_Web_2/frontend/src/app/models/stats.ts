export class Stats {
    goals: number;
    assists: number;
    matchesPlayed: number;
    yellowCards: number;
    redCards: number;

    constructor(goals: number, assists: number, matchesPlayed: number, yellowCards: number, redCards: number) {
        this.goals = goals;
        this.assists = assists;
        this.matchesPlayed = matchesPlayed;
        this.yellowCards = yellowCards;
        this.redCards = redCards;
    }
}