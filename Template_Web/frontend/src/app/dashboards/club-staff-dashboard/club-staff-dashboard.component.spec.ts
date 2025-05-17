import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ClubStaffDashboardComponent } from './club-staff-dashboard.component';

describe('ClubStaffDashboardComponent', () => {
  let component: ClubStaffDashboardComponent;
  let fixture: ComponentFixture<ClubStaffDashboardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ClubStaffDashboardComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ClubStaffDashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
