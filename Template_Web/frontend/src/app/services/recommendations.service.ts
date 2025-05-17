import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { AuthService } from './auth.service';
import { Recommendation } from '../models/recommendation.model';

@Injectable({
  providedIn: 'root'
})
export class RecommendationsService {
  private apiUrl = 'http://localhost:5000/api';

  constructor(
    private http: HttpClient,
    private authService: AuthService
  ) {}

  // Get recommendations based on user role
  getRecommendations(): Observable<Recommendation[]> {
    const user = this.authService.getCurrentUserSync();
    const role = user?.role || 'Player';
    
    return this.http.get<Recommendation[]>(`${this.apiUrl}/recommendations?role=${role}`);
  }
  
  // Add this method to match what the component is calling
  getRecommendationsForCurrentUser(): Observable<Recommendation[]> {
    return this.getRecommendations();
  }
  
  // Connect with a recommended entity - update to accept Recommendation object
  connectWithEntity(recommendation: Recommendation | string): Observable<any> {
    const user = this.authService.getCurrentUserSync();
    const entityId = typeof recommendation === 'string' ? recommendation : recommendation.entityId;
    
    return this.http.post(`${this.apiUrl}/connections`, {
      userId: user.id,
      entityId: entityId
    });
  }
  
  // Disconnect from an entity - update to accept Recommendation object
  disconnectFromEntity(recommendation: Recommendation | string): Observable<any> {
    const user = this.authService.getCurrentUserSync();
    const entityId = typeof recommendation === 'string' ? recommendation : recommendation.entityId;
    
    return this.http.delete(`${this.apiUrl}/connections/${user.id}/${entityId}`);
  }
}