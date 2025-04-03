export class Player {
    id: number;
    name: string;
    position: string;
    stats: any; // You can replace 'any' with a specific type if you have a Stats model

    constructor(id: number, name: string, position: string, stats: any) {
        this.id = id;
        this.name = name;
        this.position = position;
        this.stats = stats;
    }
}