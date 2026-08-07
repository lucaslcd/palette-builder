import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TitleEdit } from './title-edit';

describe('TitleEdit', () => {
  let component: TitleEdit;
  let fixture: ComponentFixture<TitleEdit>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TitleEdit],
    }).compileComponents();

    fixture = TestBed.createComponent(TitleEdit);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
