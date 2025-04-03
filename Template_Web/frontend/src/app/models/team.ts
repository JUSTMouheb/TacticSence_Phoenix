export class Team {
    id: number;
    name: string;
    players: string[];

    constructor(id: number, name: string, players: string[]) {
        this.id = id;
        this.name = name;
        this.players = players;
    }
}