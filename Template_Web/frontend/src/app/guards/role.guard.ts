
import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, Router } from '@angular/router';
import { AuthService } from '../services/auth.service';

@Injectable({
  providedIn: 'root'
})
export class RoleGuard implements CanActivate {
  
  constructor(private authService: AuthService, private router: Router) {}
  
  canActivate(route: ActivatedRouteSnapshot): boolean {
    const expectedRoles = route.data.roles;
    const user = this.authService.getCurrentUserSync();
    
    if (user && expectedRoles.includes(user.role)) {
      return true;
    } else {
      // If user has a different role, redirect to their proper dashboard
      if (user && user.role) {
        switch(user.role) {
          case 'Player':
            this.router.navigate(['/player-dashboard']);
            break;
          case 'Agent':
            this.router.navigate(['/agent-dashboard']);
            break;
          case 'Club Staff':
            this.router.navigate(['/club-dashboard']);
            break;
          case 'Service Provider':
            this.router.navigate(['/service-provider-dashboard']);
            break;
          default:
            this.router.navigate(['/']);
        }
      } else {
        this.router.navigate(['/']);
      }
      return false;
    }
  }
}