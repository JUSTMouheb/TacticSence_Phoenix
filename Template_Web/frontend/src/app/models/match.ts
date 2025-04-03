export class Match {
    id: number;
    date: Date;
    teams: string[];
    score: string;

    constructor(id: number, date: Date, teams: string[], score: string) {
        this.id = id;
        this.date = date;
        this.teams = teams;
        this.score = score;
    }
}