import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { map, catchError } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class CsvService {
  private playerAgentsCache: any[] | null = null;
  
  constructor(private http: HttpClient) { }
  
  getPlayerAgentsData(): Observable<any[]> {
    if (this.playerAgentsCache) {
      return of(this.playerAgentsCache);
    }
    
    return this.http.get('assets/data/Players_Agents_Dataset.csv', { responseType: 'text' })
      .pipe(
        map(csv => {
          const result = this.parseCsv(csv);
          this.playerAgentsCache = result;
          return result;
        }),
        catchError(error => {
          console.error('Error loading CSV data:', error);
          return of([]);
        })
      );
  }
  
  getPlayerAgentById(licenseNumber: string): Observable<any | null> {
    return this.getPlayerAgentsData().pipe(
      map(agents => agents.find(agent => agent.license_number === licenseNumber) || null)
    );
  }
  
  private parseCsv(csv: string): any[] {
    const lines = csv.split('\n');
    if (lines.length <= 1) {
      return [];
    }
    
    const headers = lines[0].split(',').map(h => h.trim());
    const result = [];
    
    for (let i = 1; i < lines.length; i++) {
      if (!lines[i].trim()) continue;
      
      // Handle commas within quoted values
      const row: any = {};
      let inQuotes = false;
      let currentValue = '';
      let currentHeader = 0;
      
      for (let j = 0; j < lines[i].length; j++) {
        const char = lines[i][j];
        
        if (char === '"') {
          inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
          row[headers[currentHeader]] = currentValue.trim();
          currentValue = '';
          currentHeader++;
        } else {
          currentValue += char;
        }
      }
      
      // Add the last value
      if (currentHeader < headers.length) {
        row[headers[currentHeader]] = currentValue.trim();
      }
      
      result.push(row);
    }
    
    return result;
  }
}